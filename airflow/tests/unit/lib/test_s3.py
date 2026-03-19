from dags.lib.utils.s3 import get_upload_id, list_multipart_uploads, load_file
import pytest
from unittest.mock import MagicMock


def test_get_upload_id_returns_id_from_valid_dictionary():
    upload = {"UploadId": "abc123"}
    assert get_upload_id(upload) == "abc123"


def test_get_upload_id_raises_error_when_missing_upload_id_key():
    upload = {"SomeOtherKey": "value"}
    with pytest.raises(KeyError):
        get_upload_id(upload)


def test_list_multipart_uploads_returns_uploads_when_present():
    mock_s3_hook = MagicMock()
    mock_s3_client = MagicMock()
    uploads = [
        {"UploadId": "id1", "Key": "prefix/file1"},
        {"UploadId": "id2", "Key": "prefix/file2"},
    ]
    mock_s3_client.list_multipart_uploads.return_value = {"Uploads": uploads}
    mock_s3_hook.get_conn.return_value = mock_s3_client

    result = list_multipart_uploads(mock_s3_hook, "bucket", "prefix")
    assert result == uploads
    mock_s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="prefix")


def test_list_multipart_uploads_returns_empty_list_when_no_uploads():
    mock_s3_hook = MagicMock()
    mock_s3_client = MagicMock()
    mock_s3_client.list_multipart_uploads.return_value = {"somekey": "somevalue"}  # No 'Uploads' key in response
    mock_s3_hook.get_conn.return_value = mock_s3_client

    result = list_multipart_uploads(mock_s3_hook, "bucket", "prefix")
    assert result == []
    mock_s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="prefix")


def test_load_file_uploads_file_to_s3():
    mock_s3 = MagicMock()
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"

    load_file(mock_s3, s3_bucket, dest_s3_key, local_file_name)

    mock_s3.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)

    # Since md5_hash is not provided, load_string should not be called
    mock_s3.load_string.assert_not_called()


def test_load_file_uploads_md5_file_when_md5_hash_provided():
    mock_s3 = MagicMock()
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"
    md5_hash = "abc123"

    from dags.lib.utils.s3 import load_file
    load_file(mock_s3, s3_bucket, dest_s3_key, local_file_name, md5_hash=md5_hash)

    mock_s3.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)
    mock_s3.load_string.assert_called_once_with(md5_hash, f"{dest_s3_key}.md5", s3_bucket, replace=True)
