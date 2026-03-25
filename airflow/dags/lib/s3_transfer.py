"""
This module contains advanced logic for complex data transfer operations,
such as multipart uploads with resume.

For simple S3 utility functions (e.g., basic upload, download, or helpers),
prefer using `utils/s3.py`.
"""

import logging

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient

from dags.lib.utils.humanize import bytes_to_human_readable as human_readable
from dags.lib.utils.s3 import create_multipart_upload, get_first_s3_multipart_upload_id


def multipart_upload_with_resume(
    s3: S3Hook, s3_bucket: str, s3_key: str, url: str, headers: dict | None = None, partSizeMb: int = 200
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
        partSizeMb (int, optional): Size of each upload part in MB. Default is 200MB.

    Raises:
        Exception: Any error during upload will be logged and re-raised.
    """
    try:
        s3_client = s3.get_conn()
        headers = headers or {}

        # Prepare or resume a multipart upload session
        (upload_id, parts, part_number, uploaded_bytes) = _prepare_multipart_upload(s3, s3_bucket, s3_key)
        if uploaded_bytes > 0:
            headers["Range"] = f"bytes={uploaded_bytes}-"

        # Upload remaining bytes in chunks (parts)
        with requests.get(url, stream=True, headers=headers) as r:
            # If resuming, ensure we get a partial content response (206)
            if len(parts) > 0 and r.status_code != 206:
                logging.info("File cannot be resumed, starting from the beginning")
                parts, part_number, uploaded_bytes = [], 1, 0

            file_size = int(r.headers["Content-Length"]) + uploaded_bytes
            r.raise_for_status()

            _log_upload_start(uploaded_bytes, file_size, url, s3_key)

            _upload_chunks(
                response=r,
                s3_client=s3_client,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                upload_id=upload_id,
                part_number=part_number,
                uploaded_bytes=uploaded_bytes,
                file_size=file_size,
                parts=parts,
                partSizeMb=partSizeMb,
            )

        # Complete the upload
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        logging.info(f"Multipart upload of {s3_key} from {url} completed successfully")

    except Exception as e:
        logging.error(f"Error during multipart upload: {e}")
        raise e


def _prepare_multipart_upload(s3: S3Hook, s3_bucket: str, s3_key: str):
    """
    Prepare or resume a multipart upload session.

    Checks if a multipart upload already exists for the given S3 key.
    If so, retrieves the upload state and sets the HTTP Range header to resume.
    Otherwise, creates a new multipart upload.

    Returns:
        Tuple containing:
            - upload_id (str): The multipart upload ID.
            - parts (list): List of already uploaded parts.
            - part_number (int): Next part number to use.
            - uploaded_bytes (int): Number of bytes already uploaded.
            - headers (dict): HTTP headers (possibly updated with Range).
    """
    parts = []
    part_number = 1
    uploaded_bytes = 0

    # Check if an UploadId already exists to resume download
    upload_id = get_first_s3_multipart_upload_id(s3, s3_bucket, s3_key)
    if upload_id:
        (uploaded_bytes, parts, part_number) = _get_uploaded_parts_info(s3, s3_bucket, s3_key, upload_id)
    else:
        upload_id = create_multipart_upload(s3, s3_bucket, s3_key)
    return (upload_id, parts, part_number, uploaded_bytes)


def _get_uploaded_parts_info(s3: S3Hook, s3_bucket: str, s3_key: str, upload_id: str):
    """
    Retrieve information about already uploaded parts for a multipart upload.

    Returns:
        Tuple containing:
            - uploaded_bytes (int): Total bytes already uploaded.
            - parts (list): List of part metadata.
            - part_number (int): Next part number to use.
    """
    s3_client = s3.get_conn()
    dict = s3_client.list_parts(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)
    retrieved_parts = dict.get("Parts", [])
    parts = [{"PartNumber": part["PartNumber"], "ETag": part["ETag"]} for part in retrieved_parts]
    part_number = len(parts) + 1
    uploaded_bytes = sum(part["Size"] for part in retrieved_parts)
    return (uploaded_bytes, parts, part_number)


def _upload_chunks(
    response: requests.Response,
    s3_client: BaseClient,
    s3_bucket: str,
    s3_key: str,
    upload_id: str,
    part_number: int,
    uploaded_bytes: int,
    file_size: int,
    parts: list[dict],
    partSizeMb: int,
):
    """
    Uploads file data in chunks (parts) to S3 as part of a multipart upload.

    Args:
        r: The streaming HTTP response object.
        s3_client: The boto3 S3 client.
        s3_bucket (str): Target S3 bucket.
        s3_key (str): Target S3 key.
        upload_id (str): Multipart upload ID.
        part_number (int): Starting part number.
        uploaded_bytes (int): Bytes already uploaded.
        file_size (int): Total file size.
        parts (list): List to append part metadata to.
        partSizeMb (int): Size of each part in MB.
    """
    for chunk in response.iter_content(chunk_size=int(partSizeMb * 1024 * 1024)):
        if chunk:  # filter out keep-alive new chunks
            part_response = s3_client.upload_part(
                Bucket=s3_bucket, Key=s3_key, PartNumber=part_number, UploadId=upload_id, Body=chunk
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})
            part_number += 1
            uploaded_bytes += len(chunk)
            percentage = (uploaded_bytes / file_size) * 100
            logging.info(
                f"Uploaded {human_readable(uploaded_bytes)} of {human_readable(file_size)} ({percentage:.2f}%)"
            )


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
