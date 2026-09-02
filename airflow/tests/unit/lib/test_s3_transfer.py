import io
import zipfile

import pytest
import requests
import requests_mock

from opendatalake.lib.s3_transfer import multipart_upload_with_resume, stream_unzip_to_s3


def _make_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in members.items():
            z.writestr(name, data)
    return buf.getvalue()


def _prime_multipart(s3_client):
    s3_client.list_multipart_uploads.return_value = {"Uploads": []}
    s3_client.create_multipart_upload.return_value = {"UploadId": "uid"}
    s3_client.upload_part.return_value = {"ETag": "etag"}
    s3_client.complete_multipart_upload.return_value = None


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
    s3_client.list_multipart_uploads.return_value = {"Uploads": []}
    s3_client.create_multipart_upload.return_value = {"UploadId": "upload-id"}

    with requests_mock.Mocker() as m:
        url = "http://example.com/file"
        content = b"abcde"

        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        multipart_upload_with_resume(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_key="key",
            url=url,
            part_size_mb=(1 / (1024 * 1024)),  # using part size of 1 byte
        )

        # Should call list_multipart_upload correctly
        s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="key")

        # Should create new multipart upload since no existing upload ID
        s3_client.create_multipart_upload.assert_called_once_with(Bucket="bucket", Key="key")

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
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": "entity_tag1"},
                    {"PartNumber": 2, "ETag": "entity_tag2"},
                    {"PartNumber": 3, "ETag": "entity_tag3"},
                    {"PartNumber": 4, "ETag": "entity_tag4"},
                    {"PartNumber": 5, "ETag": "entity_tag5"},
                ]
            },
        )


def test_multipart_upload_with_resume_resume_upload(s3_hook, s3_client):
    # Simulate already uploaded part
    s3_client.list_parts.return_value = {"Parts": [{"PartNumber": 1, "ETag": "entity_tag1", "Size": 1}]}
    s3_client.upload_part.side_effect = [
        {"ETag": "entity_tag2"},
        {"ETag": "entity_tag3"},
        {"ETag": "entity_tag4"},
        {"ETag": "entity_tag5"},
    ]
    s3_client.list_multipart_uploads.return_value = {"Uploads": [{"UploadId": "upload-id"}]}

    with requests_mock.Mocker() as m:
        url = "http://example.com/file"
        remaining_content = b"bcde"
        m.get(url, content=remaining_content, status_code=206, headers={"Content-Length": str(len(remaining_content))})

        multipart_upload_with_resume(
            s3=s3_hook, s3_bucket="bucket", s3_key="key", url=url, part_size_mb=(1 / (1024 * 1024))
        )

        # Check that list_multipart_uploads is called correctly
        s3_client.list_multipart_uploads.assert_called_once_with(Bucket="bucket", Prefix="key")

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
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": "entity_tag1"},
                    {"PartNumber": 2, "ETag": "entity_tag2"},
                    {"PartNumber": 3, "ETag": "entity_tag3"},
                    {"PartNumber": 4, "ETag": "entity_tag4"},
                    {"PartNumber": 5, "ETag": "entity_tag5"},
                ]
            },
        )


def test_multipart_upload_with_resume_restart_on_non_206(s3_hook, s3_client):
    s3_client.list_parts.return_value = {"Parts": [{"PartNumber": 1, "ETag": "etag", "Size": 1}]}
    s3_client.upload_part.side_effect = [
        {"ETag": "entity_tag1"},
        {"ETag": "entity_tag2"},
        {"ETag": "entity_tag3"},
        {"ETag": "entity_tag4"},
        {"ETag": "entity_tag5"},
    ]
    s3_client.list_multipart_uploads.return_value = {"Uploads": [{"UploadId": "upload-id"}]}

    with requests_mock.Mocker() as m:
        url = "http://example.com/file"
        content = b"abcde"
        # Simulate server not supporting resume (status 200 instead of 206) and sending full file content
        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        multipart_upload_with_resume(
            s3=s3_hook, s3_bucket="bucket", s3_key="key", url=url, part_size_mb=(1 / (1024 * 1024))
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
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": "entity_tag1"},
                    {"PartNumber": 2, "ETag": "entity_tag2"},
                    {"PartNumber": 3, "ETag": "entity_tag3"},
                    {"PartNumber": 4, "ETag": "entity_tag4"},
                    {"PartNumber": 5, "ETag": "entity_tag5"},
                ]
            },
        )


def test_multipart_upload_with_resume_raises_on_error(s3_hook, s3_client):
    s3_client.upload_part.side_effect = Exception("S3 error")
    s3_client.list_multipart_uploads.return_value = {"Uploads": []}
    s3_client.create_multipart_upload.return_value = {"UploadId": "upload_id"}
    with requests_mock.Mocker() as m:
        url = "http://example.com/file"
        content = b"abcde"
        m.get(url, content=content, status_code=200, headers={"Content-Length": str(len(content))})

        with pytest.raises(Exception, match="S3 error"):
            multipart_upload_with_resume(s3_hook, "bucket", "key", url)


def test_multipart_upload_with_resume_http_error_raises(s3_hook, s3_client):
    s3_client.list_parts.return_value = {"Parts": []}
    s3_client.list_multipart_uploads.return_value = {"Uploads": []}
    s3_client.create_multipart_upload.return_value = {"UploadId": "upload_id"}

    with requests_mock.Mocker() as m:
        url = "http://example.com/file"

        # Simulate HTTP error (e.g., 404 Not Found)
        m.get(url, status_code=404, headers={"Content-Length": "0"})

        with pytest.raises(requests.exceptions.HTTPError):
            multipart_upload_with_resume(
                s3=s3_hook, s3_bucket="bucket", s3_key="key", url=url, part_size_mb=int(1 / (1024 * 1024))
            )


def test_stream_unzip_to_s3_uploads_only_matching_members(s3_hook, s3_client):
    _prime_multipart(s3_client)
    zip_bytes = _make_zip(
        {
            "dbNSFP4.9a_variant.chr1.gz": b"chr1-data",
            "dbNSFP4.9a_variant.chr2.gz": b"chr2-data",
            "dbNSFP4.9a_gene.gz": b"gene-data",
            "dbNSFP4.9a.readme.txt": b"readme",
        }
    )

    with requests_mock.Mocker() as m:
        url = "http://example.com/dbNSFP4.9a.zip"
        m.get(url, content=zip_bytes, status_code=200)

        uploaded = stream_unzip_to_s3(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_prefix="raw/landing/dbnsfp/4.9a",
            url=url,
            member_pattern="*_variant.chr*.gz",
        )

    assert set(uploaded) == {"dbNSFP4.9a_variant.chr1.gz", "dbNSFP4.9a_variant.chr2.gz"}
    # One multipart upload per matching member; skipped members are drained, not uploaded.
    assert s3_client.create_multipart_upload.call_count == 2
    uploaded_keys = {call.kwargs["Key"] for call in s3_client.create_multipart_upload.call_args_list}
    assert uploaded_keys == {
        "raw/landing/dbnsfp/4.9a/dbNSFP4.9a_variant.chr1.gz",
        "raw/landing/dbnsfp/4.9a/dbNSFP4.9a_variant.chr2.gz",
    }
    assert s3_client.complete_multipart_upload.call_count == 2


def test_stream_unzip_to_s3_uploads_all_members_without_pattern(s3_hook, s3_client):
    _prime_multipart(s3_client)
    zip_bytes = _make_zip({"a.gz": b"a", "b.txt": b"b"})

    with requests_mock.Mocker() as m:
        url = "http://example.com/a.zip"
        m.get(url, content=zip_bytes, status_code=200)
        uploaded = stream_unzip_to_s3(s3=s3_hook, s3_bucket="bucket", s3_prefix="p", url=url)

    assert set(uploaded) == {"a.gz", "b.txt"}


def test_stream_unzip_to_s3_raises_when_no_member_matches(s3_hook, s3_client):
    _prime_multipart(s3_client)
    zip_bytes = _make_zip({"readme.txt": b"x"})

    with requests_mock.Mocker() as m:
        url = "http://example.com/a.zip"
        m.get(url, content=zip_bytes, status_code=200)
        with pytest.raises(ValueError, match="No zip members matched"):
            stream_unzip_to_s3(s3=s3_hook, s3_bucket="bucket", s3_prefix="p", url=url, member_pattern="*.gz")

    s3_client.create_multipart_upload.assert_not_called()


def test_stream_unzip_to_s3_rejects_non_url(s3_hook, s3_client):
    # A mis-pasted param (e.g. log text) must fail fast, before any HTTP/S3 work.
    with pytest.raises(ValueError, match="requires an http\\(s\\) URL"):
        stream_unzip_to_s3(s3=s3_hook, s3_bucket="bucket", s3_prefix="p", url="not a url", member_pattern="*.gz")
    s3_hook.get_conn.assert_not_called()


def test_stream_unzip_to_s3_http_error_raises(s3_hook, s3_client):
    with requests_mock.Mocker() as m:
        url = "http://example.com/a.zip"
        m.get(url, status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            stream_unzip_to_s3(s3=s3_hook, s3_bucket="bucket", s3_prefix="p", url=url, member_pattern="*.gz")
