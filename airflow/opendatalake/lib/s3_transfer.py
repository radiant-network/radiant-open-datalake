"""
This module contains advanced logic for complex data transfer operations,
such as multipart uploads with resume.

For simple S3 utility functions (e.g., basic upload, download, or helpers),
prefer using `utils/s3.py`.
"""

import fnmatch
import logging

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from opendatalake.lib.utils.humanize import bytes_to_human_readable as human_readable
from opendatalake.lib.utils.s3 import MultipartUpload


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


def stream_unzip_to_s3(
    s3: S3Hook,
    s3_bucket: str,
    s3_prefix: str,
    url: str,
    headers: dict | None = None,
    member_pattern: str | None = None,
    part_size_mb: int = 200,
    read_chunk_mb: int = 8,
) -> list[str]:

    url = (url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        raise ValueError(f"stream_unzip_to_s3 requires an http(s) URL; got: {url[:80]!r}")

    # Lazy import because we don't want MWAA to import this module
    from stream_unzip import stream_unzip

    headers = headers or {}
    part_size = round(part_size_mb * 1024 * 1024)
    read_chunk = round(read_chunk_mb * 1024 * 1024)
    s3_client = s3.get_conn()
    uploaded: list[str] = []

    with requests.get(url, stream=True, headers=headers) as r:
        r.raise_for_status()
        logging.info(f"Start stream-unzip of '{url}' into s3://{s3_bucket}/{s3_prefix}/")
        for member_name, _uncompressed_size, member_chunks in stream_unzip(r.iter_content(chunk_size=read_chunk)):
            base = _member_basename(member_name)
            if not base or (member_pattern and not fnmatch.fnmatch(base, member_pattern)):
                _drain(member_chunks)
                continue
            s3_key = f"{s3_prefix}/{base}"
            _multipart_upload_chunks(s3_client, s3_bucket, s3_key, member_chunks, part_size)
            uploaded.append(base)
            logging.info(f"Uploaded zip member '{base}' to s3://{s3_bucket}/{s3_key}")

    if not uploaded:
        raise ValueError(f"No zip members matched pattern {member_pattern!r} in {url}")
    logging.info(f"Stream-unzip of '{url}' complete: {len(uploaded)} member(s) uploaded")
    return uploaded


def _member_basename(member_name) -> str:
    name = member_name.decode("utf-8", errors="replace") if isinstance(member_name, bytes) else member_name
    return name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def _drain(chunks) -> None:
    # `stream_unzip` requires a member's bytes to be fully consumed before the next member is read.
    for _ in chunks:
        pass


def _multipart_upload_chunks(s3_client, s3_bucket: str, s3_key: str, chunks, part_size: int) -> None:
    """Multipart-upload an iterator of byte chunks to S3, batching into ``part_size`` parts."""
    with MultipartUpload(s3_client, s3_bucket, s3_key) as mp:
        mp.reset()  # zip members are streamed fresh; no mid-archive resume
        buffer = bytearray()
        for chunk in chunks:
            buffer.extend(chunk)
            while len(buffer) >= part_size:
                mp.upload_part(bytes(buffer[:part_size]))
                del buffer[:part_size]
        # Flush the tail; guarantee at least one part so an empty member still completes.
        if buffer or not mp.uploaded_parts:
            mp.upload_part(bytes(buffer))


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
