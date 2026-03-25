from unittest.mock import patch

import pytest
import requests
import requests_mock

from dags.lib.s3_transfer import multipart_upload_with_resume


def test_multipart_upload_with_resume_new_upload(s3_hook, s3_client):
    s3_client.list_parts.return_value = {"Parts": []}
    s3_client.complete_multipart_upload.return_value = None
    s3_client.upload_part.side_effect = [
        {"ETag": "entity_tag1"},
        {"ETag": "entity_tag2"},
        {"ETag": "entity_tag3"},
        {"ETag": "entity_tag4"},
        {"ETag": "entity_tag5"},
    ]

    with (
        patch(
            "dags.lib.s3_transfer.get_first_s3_multipart_upload_id",
            return_value=None
        ),
        patch(
            "dags.lib.s3_transfer.create_multipart_upload",
            return_value="upload-id") as create_multipart_upload_mock,
        requests_mock.Mocker() as m
    ):
        url = "http://example.com/file"
        content = b"abcde"
        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        multipart_upload_with_resume(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_key="key",
            url=url,
            partSizeMb=(1 / (1024 * 1024))  # using part size of 1 byte
        )

        # Should create new multipart upload since no existing upload ID
        create_multipart_upload_mock.assert_called_once_with(
            s3_hook, "bucket", "key"
        )

        # Check that Range header is NOT present in the request
        last_request = m.last_request
        assert "Range" not in last_request.headers

        # Should call upload_part exactly 5 times (one per byte)
        assert s3_client.upload_part.call_count == 5

        # Should call complete multipart
        s3_client.complete_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key="key",
            UploadId="upload-id",
            MultipartUpload={'Parts': [
                {"PartNumber": 1, "ETag": "entity_tag1"},
                {"PartNumber": 2, "ETag": "entity_tag2"},
                {"PartNumber": 3, "ETag": "entity_tag3"},
                {"PartNumber": 4, "ETag": "entity_tag4"},
                {"PartNumber": 5, "ETag": "entity_tag5"},
            ]}
        )


def test_multipart_upload_with_resume_resume_upload(s3_hook, s3_client):
    # Simulate already uploaded part
    s3_client.list_parts.return_value = {
        "Parts": [{"PartNumber": 1, "ETag": "entity_tag1", "Size": 1}]
    }
    s3_client.complete_multipart_upload.return_value = None
    s3_client.upload_part.side_effect = [
        {"ETag": "entity_tag2"},
        {"ETag": "entity_tag3"},
        {"ETag": "entity_tag4"},
        {"ETag": "entity_tag5"},
    ]

    with (
        patch("dags.lib.s3_transfer.get_first_s3_multipart_upload_id", return_value="upload-id"),
        requests_mock.Mocker() as m
    ):
        url = "http://example.com/file"
        remaining_content = b"bcde"
        m.get(url, content=remaining_content, status_code=206, headers={"Content-Length": str(len(remaining_content))})

        multipart_upload_with_resume(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_key="key",
            url=url,
            partSizeMb=(1 / (1024 * 1024))
        )
        last_request = m.last_request
        assert "Range" in last_request.headers
        assert last_request.headers["Range"] == "bytes=1-"

        # Should call upload_part exactly 4 times (one per remaining byte)
        assert s3_client.upload_part.call_count == 4

        # Should call complete multipart upload
        s3_client.complete_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key="key",
            UploadId="upload-id",
            MultipartUpload={'Parts': [
                {"PartNumber": 1, "ETag": "entity_tag1"},
                {"PartNumber": 2, "ETag": "entity_tag2"},
                {"PartNumber": 3, "ETag": "entity_tag3"},
                {"PartNumber": 4, "ETag": "entity_tag4"},
                {"PartNumber": 5, "ETag": "entity_tag5"},
            ]}
        )


def test_multipart_upload_with_resume_restart_on_non_206(s3_hook, s3_client):
    s3_client.list_parts.return_value = {
        "Parts": [{"PartNumber": 1, "ETag": "etag", "Size": 1}]
    }
    s3_client.complete_multipart_upload.return_value = None
    s3_client.upload_part.side_effect = [
        {"ETag": "entity_tag1"},
        {"ETag": "entity_tag2"},
        {"ETag": "entity_tag3"},
        {"ETag": "entity_tag4"},
        {"ETag": "entity_tag5"},
    ]

    with (
        patch("dags.lib.s3_transfer.get_first_s3_multipart_upload_id", return_value="upload-id"),
        requests_mock.Mocker() as m
    ):

        url = "http://example.com/file"
        content = b"abcde"
        # Simulate server not supporting resume (status 200 instead of 206) and sending full file content
        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        multipart_upload_with_resume(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_key="key",
            url=url,
            partSizeMb=(1 / (1024 * 1024))
        )
        assert s3_client.upload_part.called
        assert s3_client.complete_multipart_upload.called

        # Should call upload_part exactly 5 times (one per byte)
        assert s3_client.upload_part.call_count == 5

        # Should call complete multipart upload
        s3_client.complete_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key="key",
            UploadId="upload-id",
            MultipartUpload={'Parts': [
                {"PartNumber": 1, "ETag": "entity_tag1"},
                {"PartNumber": 2, "ETag": "entity_tag2"},
                {"PartNumber": 3, "ETag": "entity_tag3"},
                {"PartNumber": 4, "ETag": "entity_tag4"},
                {"PartNumber": 5, "ETag": "entity_tag5"},
            ]}
        )


def test_multipart_upload_with_resume_raises_on_error(s3_hook, s3_client):
    s3_client.upload_part.side_effect = Exception("S3 error")

    with (
        patch("dags.lib.s3_transfer.get_first_s3_multipart_upload_id", return_value=None),
        patch("dags.lib.s3_transfer.create_multipart_upload", return_value="upload-id"),
        requests_mock.Mocker() as m
    ):
        url = "http://example.com/file"
        content = b"abcde"
        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        with pytest.raises(Exception,  match="S3 error"):
            multipart_upload_with_resume(s3_hook, "bucket", "key", url)


def test_multipart_upload_with_resume_http_error_raises(s3_hook, s3_client):
    s3_client.list_parts.return_value = {"Parts": []}

    with (
        patch("dags.lib.s3_transfer.get_first_s3_multipart_upload_id", return_value=None),
        patch("dags.lib.s3_transfer.create_multipart_upload", return_value="upload-id"),
        requests_mock.Mocker() as m
    ):
        url = "http://example.com/file"

        # Simulate HTTP error (e.g., 404 Not Found)
        m.get(url, status_code=404, headers={"Content-Length": "0"})

        with pytest.raises(requests.exceptions.HTTPError):
            multipart_upload_with_resume(
                s3=s3_hook,
                s3_bucket="bucket",
                s3_key="key",
                url=url,
                partSizeMb=(1 / (1024 * 1024))
            )
