"""
This module contains advanced logic for complex data transfer operations,
such as multipart uploads with resume.

For simple S3 utility functions (e.g., basic upload, download, or helpers),
prefer using `utils/s3.py`.
"""

import logging

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.lib.utils.humanize import bytes_to_human_readable as human_readable
from dags.lib.utils.s3 import MultipartUpload


def multipart_upload_with_resume(
    s3: S3Hook, s3_bucket: str, s3_key: str, url: str, headers: dict | None = None, part_size_mb: int = 200
) -> None:
    """
    Perform a multipart upload to S3 with resume capability from a remote URL.

    This function will:
    - Check if a multipart upload already exists for the given S3 key and resume if possible.
    - Download the remote file in chunks and upload each chunk as a part to S3.
    - Log progress and handle both new and resumed uploads.
    - Complete the multipart upload once all parts are uploaded.

    Args:
        s3 (S3Hook): Airflow S3Hook instance.
        s3_bucket (str): Target S3 bucket.
        s3_key (str): Target S3 key (object path).
        url (str): Remote file URL to stream from.
        headers (dict, optional): HTTP headers for the request.
        part_size_mb (int, optional): Size of each upload part in MB. Default is 200MB.

    Raises:
        Exception: Any error during upload will be logged and re-raised.
    """
    try:
        headers = headers or {}

        with MultipartUpload(s3.get_conn(), s3_bucket, s3_key) as multipart_upload:
            if multipart_upload.uploaded_bytes > 0:
                headers["Range"] = f"bytes={multipart_upload.uploaded_bytes}-"

            # Upload remaining bytes in chunks (parts)
            with requests.get(url, stream=True, headers=headers) as r:
                # If resuming, ensure we get a partial content response (206)
                if len(multipart_upload.uploaded_parts) > 0 and r.status_code != 206:
                    logging.info("File cannot be resumed, starting from the beginning")
                    multipart_upload.reset()

                r.raise_for_status()

                file_size = int(r.headers["Content-Length"]) + multipart_upload.uploaded_bytes
                _log_upload_start(multipart_upload.uploaded_bytes, file_size, url, s3_key)

                for chunk in r.iter_content(chunk_size=round(part_size_mb * 1024 * 1024)):
                    if chunk:  # filter out keep-alive new chunks
                        multipart_upload.upload_part(chunk)
                        _log_progress(multipart_upload.uploaded_bytes, file_size)

        logging.info(f"Multipart upload of {s3_key} from {url} completed successfully")

    except Exception as e:
        logging.error(f"Error during multipart upload: {e}")
        raise e


def _log_progress(uploaded_bytes: int, file_size: int) -> None:
    percentage = (uploaded_bytes / file_size) * 100
    logging.info(f"Uploaded {human_readable(uploaded_bytes)} of {human_readable(file_size)} ({percentage:.2f}%)")


def _log_upload_start(uploaded_bytes: int, file_size: int, url: str, s3_key: str) -> None:
    """
    Log the start or resume of an upload.

    Args:
        uploaded_bytes (int): Number of bytes already uploaded.
        file_size (int): Total file size.
        url (str): Source URL.
        s3_key (str): Target S3 key.
    """
    if uploaded_bytes == 0:
        logging.info(f"Start upload of '{url}' ({human_readable(file_size)}), to {s3_key}")
    else:
        logging.info(
            f"Resuming upload of '{url}' ({human_readable(file_size)}), to {s3_key} "
            f"({human_readable(uploaded_bytes)} already downloaded)"
        )
