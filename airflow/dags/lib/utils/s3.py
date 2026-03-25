import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_upload_id(upload: dict) -> str:
    """
    Extract the UploadId from a multipart upload metadata dictionary.

    Args:
        upload (dict): A dictionary representing a multipart upload, as returned by S3.

    Returns:
        str: The UploadId of the multipart upload.
    """
    return upload["UploadId"]


def list_multipart_uploads(s3: S3Hook, s3_bucket: str, s3_key: str) -> list[dict]:
    """
    Retrieve a list of active multipart uploads for the specified S3 key prefix.

    Args:
        s3 (S3Hook): The S3Hook instance to use for the connection.
        s3_bucket (str): The name of the S3 bucket.
        s3_key (str): The key prefix to filter multipart uploads.

    Returns:
        list[dict]: A list of multipart upload metadata dictionaries.
                    Returns an empty list if no active uploads are found.
    """
    s3_client = s3.get_conn()
    response = s3_client.list_multipart_uploads(Bucket=s3_bucket, Prefix=s3_key)
    return response.get("Uploads", [])


def create_multipart_upload(s3: S3Hook, s3_bucket: str, s3_key: str) -> str:
    """
    Initiate a multipart upload and return the UploadId.

    Args:
        s3 (S3Hook): The S3Hook instance to use for the connection.
        s3_bucket (str): The name of the S3 bucket.
        s3_key (str): The key for which to initiate the multipart upload.

    Returns:
        str: The UploadId of the initiated multipart upload.
    """
    s3_client = s3.get_conn()
    response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
    return get_upload_id(response)


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
