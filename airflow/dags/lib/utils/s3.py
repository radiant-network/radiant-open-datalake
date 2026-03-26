import logging
from dataclasses import dataclass

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient

logger = logging.getLogger(__name__)


def load_file(s3: S3Hook, s3_bucket: str, dest_s3_key: str, local_file_name: str, md5_hash: str | None = None):
    """
    Upload a file to S3. Optionally upload an associated .md5 file if an MD5 hash is provided.

    Args:
        s3 (S3Hook): S3Hook instance.
        s3_bucket (str): Target S3 bucket.
        dest_s3_key (str): Destination S3 key.
        local_file_name (str): Local file to upload.
        md5_hash (str, optional): If provided, uploads a .md5 file containing this hash alongside the main file.
    """
    s3.load_file(local_file_name, dest_s3_key, s3_bucket, replace=True)
    logging.info(f"File '{local_file_name}' successfully uploaded to 's3://{s3_bucket}/{dest_s3_key}'.")
    if md5_hash:
        s3.load_string(md5_hash, f"{dest_s3_key}.md5", s3_bucket, replace=True)
        logging.info(
            f"MD5 file for '{local_file_name}.md5' successfully uploaded to 's3://{s3_bucket}/{dest_s3_key}.md5'."
        )


@dataclass
class UploadedPart:
    part_number: int
    etag: str


class MultipartUpload:
    def __init__(self, s3_client: BaseClient, bucket: str, key: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        self.upload_id: str | None = None
        self.uploaded_parts: list[UploadedPart] = []
        self.uploaded_bytes: int = 0

    def prepare(self):
        uploads = self._list_multipart_uploads()
        upload_id = uploads[0]["UploadId"] if uploads else None
        if upload_id:
            response = self.s3_client.list_parts(Bucket=self.bucket, Key=self.key, UploadId=upload_id)
            retrieved_parts = response.get("Parts", [])
            self.uploaded_parts = [
                UploadedPart(part_number=part["PartNumber"], etag=part["ETag"]) for part in retrieved_parts
            ]
            self.uploaded_bytes = sum(part["Size"] for part in retrieved_parts)
        else:
            upload_id = self._create_multipart_upload()
        self.upload_id = upload_id

    def _list_multipart_uploads(self) -> list[dict]:
        uploads = self.s3_client.list_multipart_uploads(Bucket=self.bucket, Prefix=self.key)
        return uploads.get("Uploads", [])

    def _create_multipart_upload(self) -> str:
        response = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        return response["UploadId"]

    def reset(self):
        self.uploaded_parts = []
        self.uploaded_bytes = 0

    def upload_part(self, data: bytes):
        part_number = len(self.uploaded_parts) + 1
        part_response = self.s3_client.upload_part(
            Bucket=self.bucket, Key=self.key, PartNumber=part_number, UploadId=self.upload_id, Body=data
        )
        self.uploaded_parts.append(UploadedPart(part_number=part_number, etag=part_response["ETag"]))
        self.uploaded_bytes += len(data)

    def complete(self):
        parts = [
            {"ETag": p.etag, "PartNumber": p.part_number}
            for p in sorted(self.uploaded_parts, key=lambda p: p.part_number)
        ]
        self.s3_client.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, MultipartUpload={"Parts": parts}
        )

    def __enter__(self) -> "MultipartUpload":
        self.prepare()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self.complete()
        else:
            logger.error(
                f"Exception during multipart upload for 's3://{self.bucket}/{self.key}': {exc_val}",
            )
        return False
