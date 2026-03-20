from unittest.mock import patch

import pytest

from dags.lib.utils.s3 import (
    create_multipart_upload,
    get_first_s3_multipart_upload_id,
    get_upload_id,
    list_multipart_uploads,
    load_file,
)


def test_get_upload_id_returns_id_from_valid_dictionary():
    upload = {"UploadId": "abc123"}
    assert get_upload_id(upload) == "abc123"


def test_get_upload_id_raises_error_when_missing_upload_id_key():
    upload = {"SomeOtherKey": "value"}
    with pytest.raises(KeyError):
        get_upload_id(upload)


def test_list_multipart_uploads_returns_uploads_when_present(s3_hook, s3_client):
    uploads = [
        {"UploadId": "id1", "Key": "prefix/file1"},
        {"UploadId": "id2", "Key": "prefix/file2"},
    ]
    s3_client.list_multipart_uploads.return_value = {"Uploads": uploads}

    result = list_multipart_uploads(s3_hook, "bucket", "prefix")
    assert result == uploads
    s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="prefix")


def test_list_multipart_uploads_returns_empty_list_when_no_uploads(s3_hook, s3_client):
    s3_client.list_multipart_uploads.return_value = {"somekey": "somevalue"}  # No 'Uploads' key in response

    result = list_multipart_uploads(s3_hook, "bucket", "prefix")
    assert result == []
    s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="prefix")


def test_load_file_uploads_file_to_s3(s3_hook):
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"

    load_file(s3_hook, s3_bucket, dest_s3_key, local_file_name)

    s3_hook.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)

    # Since md5_hash is not provided, load_string should not be called
    s3_hook.load_string.assert_not_called()


def test_load_file_uploads_md5_file_when_md5_hash_provided(s3_hook):
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"
    md5_hash = "abc123"

    from dags.lib.utils.s3 import load_file
    load_file(s3_hook, s3_bucket, dest_s3_key, local_file_name, md5_hash=md5_hash)

    s3_hook.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)
    s3_hook.load_string.assert_called_once_with(md5_hash, f"{dest_s3_key}.md5", s3_bucket, replace=True)


def test_get_first_s3_multipart_upload_id_returns_none_if_no_uploads(s3_hook):
    with patch("dags.lib.utils.s3.list_multipart_uploads") as list_uploads:
        list_uploads.return_value = []
        upload_id = get_first_s3_multipart_upload_id(s3_hook, "bucket", "key")
        assert upload_id is None


def test_get_first_s3_multipart_upload_id_returns_first_upload_id(s3_hook):
    with (
        patch("dags.lib.utils.s3.list_multipart_uploads") as list_uploads,
        patch("dags.lib.utils.s3.get_upload_id") as get_upload_id
    ):
        list_uploads.return_value = [
            {"UploadId": "first"},
            {"UploadId": "second"}
        ]
        get_upload_id.return_value = "first"
        upload_id = get_first_s3_multipart_upload_id(s3_hook, "bucket", "key")
        assert upload_id == "first"


def test_create_multipart_upload_returns_upload_id(s3_hook, s3_client):
    s3_client.create_multipart_upload.return_value = {"UploadId": "test-upload-id"}

    upload_id = create_multipart_upload(s3_hook, "bucket", "key")
    assert upload_id == "test-upload-id"
    s3_client.create_multipart_upload.assert_called_once_with(Bucket="bucket", Key="key")
