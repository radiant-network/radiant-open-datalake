import logging
import tarfile
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from opendatalake.lib.config import raw_datalake_bucket, s3_conn_id
from opendatalake.lib.s3_transfer import multipart_upload_with_resume
from opendatalake.lib.utils.http import http_get, stream_download_file
from opendatalake.lib.utils.md5 import check_md5, compute_file_md5, extract_md5_from_checksum_file_content
from opendatalake.lib.utils.s3 import load_file


class S3Downloader:
    """
    Handles uploading files to S3 for a given download configuration.
    """

    def __init__(self, s3_prefix, version, download_conf, s3=None, s3_bucket=None):
        self.s3_prefix = s3_prefix
        self.version = version
        self.download_conf = download_conf
        self.s3 = s3 if s3 is not None else S3Hook(s3_conn_id)
        self.s3_bucket = s3_bucket if s3_bucket is not None else raw_datalake_bucket

    def upload_via_local_copy(self):
        """
        Downloads the file locally, checks MD5 if available, extracts tar members if needed, and uploads to S3.

        Also uploads the MD5 hash to S3 if present. For tar extraction, MD5 is computed after extraction and uploaded
        for each memberif the download config has md5_present=True.
        """
        url = self.download_conf.get_url(self.version)
        dest_file_name = self.download_conf.name or Path(url).name
        md5_hash = _get_md5_hash(url) if self.download_conf.md5_present else None
        headers = self.download_conf.headers or {}

        logging.info(f"Start upload of {url}")
        stream_download_file(url=url, dest_file_name=dest_file_name, headers=headers)

        if md5_hash:
            check_md5(dest_file_name, md5_hash)
        if self.download_conf.extract_members:
            self._extract_and_upload_tar_members(
                tar_file_name=dest_file_name,
                save_md5=md5_hash is not None,
            )
        else:
            load_file(
                s3=self.s3,
                s3_bucket=self.s3_bucket,
                s3_key=f"{self.s3_prefix}/{dest_file_name}",
                local_file_name=dest_file_name,
                md5_hash=md5_hash,
            )

    def direct_upload(self) -> None:
        """
        Streams a file directly to S3 using multipart upload. Optionally uploads MD5 hash.
        """
        url = self.download_conf.get_url(self.version)
        dest_file_name = self.download_conf.name or Path(url).name
        s3_key = f"{self.s3_prefix}/{dest_file_name}"
        md5_hash = _get_md5_hash(url) if self.download_conf.md5_present else None
        headers = self.download_conf.headers or {}

        multipart_upload_with_resume(s3=self.s3, s3_bucket=self.s3_bucket, s3_key=s3_key, url=url, headers=headers)
        if md5_hash:
            self.s3.load_string(md5_hash, f"{s3_key}.md5", self.s3_bucket, replace=True)
            logging.info("Md5 file saved, but not checked (cannot be done on stream upload)")

    def _extract_and_upload_tar_members(self, tar_file_name: str, save_md5: bool):
        member_names = self.download_conf.extract_members

        with tarfile.open(tar_file_name, "r") as tar:
            tar.extractall(filter=lambda member, _: member if member.name in member_names else None)

        for member in member_names:
            s3_key = f"{self.s3_prefix}/{member}"
            md5_hash = compute_file_md5(member) if save_md5 else None
            load_file(s3=self.s3, s3_bucket=self.s3_bucket, s3_key=s3_key, local_file_name=member, md5_hash=md5_hash)


def _get_md5_hash(url):
    text = http_get(url + ".md5").text
    return extract_md5_from_checksum_file_content(text)
