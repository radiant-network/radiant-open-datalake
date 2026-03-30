import logging
import tarfile
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.lib.domain.model.download_config import DownloadConfig
from dags.lib.domain.model.sources import get_download_configs
from dags.lib.s3_transfer import multipart_upload_with_resume
from dags.lib.utils.http import http_get, stream_download_file
from dags.lib.utils.md5 import check_md5, compute_file_md5, extract_md5_from_checksum_file_content
from dags.lib.utils.s3 import load_file


def download(s3: S3Hook, s3_bucket: str, s3_prefix: str, source: str, version: str):
    for download_config in get_download_configs(source):
        _handle_file_transfer(s3, s3_bucket, s3_prefix, version, download_config)


def _handle_file_transfer(s3, s3_bucket, s3_prefix: str, version: str, download_conf: DownloadConfig):
    # Pre-compute the URL and any parameters that may depend on it.
    # This ensures all config values are consistent with the actual URL used for the download,
    # especially when the URL is generated dynamically for some sources.

    if download_conf.use_direct_upload:
        _direct_upload_to_s3(
            s3=s3, s3_bucket=s3_bucket, s3_prefix=s3_prefix, version=version, download_conf=download_conf
        )

    else:
        _upload_via_local_copy_to_s3(
            s3=s3,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            version=version,
            download_conf=download_conf,
        )


def _upload_via_local_copy_to_s3(
    s3: S3Hook, s3_bucket: str, s3_prefix: str, version: str, download_conf: DownloadConfig
):
    url = download_conf.get_url(version)
    dest_file_name = download_conf.name or Path(url).name
    md5_hash = _get_md5_hash(url) if download_conf.md5_present else None
    headers = download_conf.headers or {}

    logging.info(f"Start upload of {url}")
    stream_download_file(url=url, dest_file_name=dest_file_name, headers=headers)

    if md5_hash:
        check_md5(dest_file_name, md5_hash)
    if download_conf.extract_members:
        _extract_and_upload_tar_members(
            s3=s3,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            member_names=download_conf.extract_members,
            tar_file_name=dest_file_name,
            save_md5=md5_hash is not None,
        )
    else:
        load_file(
            s3=s3,
            s3_bucket=s3_bucket,
            s3_key=f"{s3_prefix}/{dest_file_name}",
            local_file_name=dest_file_name,
            md5_hash=md5_hash,
        )


def _direct_upload_to_s3(
    s3: S3Hook, s3_bucket: str, s3_prefix: str, version: str, download_conf: DownloadConfig
) -> None:
    url = download_conf.get_url(version)
    dest_file_name = download_conf.name or Path(url).name
    s3_key = f"{s3_prefix}/{dest_file_name}"
    md5_hash = _get_md5_hash(url) if download_conf.md5_present else None
    headers = download_conf.headers or {}

    multipart_upload_with_resume(s3=s3, s3_bucket=s3_bucket, s3_key=s3_key, url=url, headers=headers)
    if md5_hash:
        s3.load_string(md5_hash, f"{s3_key}.md5", s3_bucket, replace=True)
        logging.info("Md5 file saved, but not checked (cannot be done on stream upload)")


def _extract_and_upload_tar_members(
    s3: S3Hook, s3_bucket: str, s3_prefix: str, member_names: list[str], tar_file_name: str, save_md5: bool
):
    with tarfile.open(tar_file_name, "r") as tar:
        tar.extractall(filter=lambda member, _: member if member.name in member_names else None)

    for member in member_names:
        s3_key = f"{s3_prefix}/{member}"
        md5_hash = compute_file_md5(member) if save_md5 else None
        load_file(s3=s3, s3_bucket=s3_bucket, s3_key=s3_key, local_file_name=member, md5_hash=md5_hash)


def _get_md5_hash(url):
    text = http_get(url + ".md5").text
    return extract_md5_from_checksum_file_content(text)
