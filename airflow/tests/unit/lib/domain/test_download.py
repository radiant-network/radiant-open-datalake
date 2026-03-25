from unittest.mock import MagicMock, call, patch

from dags.lib.domain.download import download
from dags.lib.domain.model.download_config import DownloadConfig


def test_download_direct_upload_no_md5(s3_hook):
    download_config = DownloadConfig(url="http://example.com/file.txt", use_direct_upload=True, md5_present=False)

    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.http_get", return_value=MagicMock(text="abc123  file.txt")),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_multipart.assert_called_once_with(
            s3=s3_hook, s3_bucket="bucket", s3_key="prefix/file.txt", url="http://example.com/file.txt", headers=None
        )
        s3_hook.load_string.assert_not_called()


def test_download_direct_upload_with_md5(s3_hook):
    download_config = DownloadConfig(url="http://example.com/file.txt", use_direct_upload=True, md5_present=True)
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.http_get", return_value=MagicMock(text="abc123  file.txt")),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_multipart.assert_called_once_with(
            s3=s3_hook, s3_bucket="bucket", s3_key="prefix/file.txt", url="http://example.com/file.txt", headers=None
        )
        s3_hook.load_string.assert_called_once_with("abc123", "prefix/file.txt.md5", "bucket", replace=True)


def test_download_via_local_copy_no_md5(s3_hook):
    download_config = DownloadConfig(url="http://example.com/file2.txt", use_direct_upload=False, md5_present=False)
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile") as tarfile_mock,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="file2.txt", headers=None
        )
        mock_check_md5.assert_not_called()
        mock_load.assert_called_once_with(
            s3=s3_hook, s3_bucket="bucket", dest_s3_key="prefix/file2.txt", local_file_name="file2.txt", md5_hash=None
        )
        tarfile_mock.open.assert_not_called()


def test_download_via_local_copy_with_md5(s3_hook):
    download_config = DownloadConfig(url="http://example.com/file2.txt", use_direct_upload=False, md5_present=True)
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.http_get", return_value=MagicMock(text="abc123  file2.txt")),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile") as tarfile_mock,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="file2.txt", headers=None
        )
        mock_check_md5.assert_called_once_with("file2.txt", "abc123")
        mock_load.assert_called_once_with(
            s3=s3_hook,
            s3_bucket="bucket",
            dest_s3_key="prefix/file2.txt",
            local_file_name="file2.txt",
            md5_hash="abc123",
        )
        tarfile_mock.open.assert_not_called()


def test_download_via_local_copy_with_extract_members_no_md5(s3_hook):
    download_config = DownloadConfig(
        url="http://example.com/archive.tar.gz",
        use_direct_upload=False,
        md5_present=False,
        extract_members=["file1.txt", "file2.txt"],
    )
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile.open") as mock_tarfile_open,
    ):
        mock_tar = MagicMock()
        mock_tarfile_open.return_value.__enter__.return_value = mock_tar

        download(s3_hook, "bucket", "prefix", "source")

        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/archive.tar.gz", dest_file_name="archive.tar.gz", headers=None
        )
        mock_tar.extract.assert_has_calls([call("file1.txt"), call("file2.txt")], any_order=True)
        mock_load.assert_has_calls(
            [
                call(
                    s3=s3_hook,
                    s3_bucket="bucket",
                    s3_key="prefix/file1.txt",
                    local_file_name="file1.txt",
                    md5_hash=None,
                ),
                call(
                    s3=s3_hook,
                    s3_bucket="bucket",
                    s3_key="prefix/file2.txt",
                    local_file_name="file2.txt",
                    md5_hash=None,
                ),
            ],
            any_order=True,
        )


def test_download_via_local_copy_with_extract_members_with_md5(s3_hook):
    download_config = DownloadConfig(
        url="http://example.com/archive.tar.gz",
        use_direct_upload=False,
        md5_present=True,
        extract_members=["file1.txt", "file2.txt"],
    )
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.http_get", return_value=MagicMock(text="abc123  archive.tar")),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.compute_file_md5", side_effect=["abcde", "fghij"]),
        patch("dags.lib.domain.download.check_md5"),
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile.open") as mock_tarfile_open,
    ):
        mock_tar = MagicMock()
        mock_tarfile_open.return_value.__enter__.return_value = mock_tar

        download(s3_hook, "bucket", "prefix", "source")

        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/archive.tar.gz", dest_file_name="archive.tar.gz", headers=None
        )
        mock_tar.extract.assert_has_calls([call("file1.txt"), call("file2.txt")], any_order=True)
        mock_load.assert_has_calls(
            [
                call(
                    s3=s3_hook,
                    s3_bucket="bucket",
                    s3_key="prefix/file1.txt",
                    local_file_name="file1.txt",
                    md5_hash="abcde",
                ),
                call(
                    s3=s3_hook,
                    s3_bucket="bucket",
                    s3_key="prefix/file2.txt",
                    local_file_name="file2.txt",
                    md5_hash="fghij",
                ),
            ],
            any_order=True,
        )


def test_download_direct_upload_with_configured_name_and_headers(s3_hook):
    download_config = DownloadConfig(
        url="http://example.com/file.txt",
        use_direct_upload=True,
        md5_present=False,
        name="custom_name.txt",
        headers={"myheader": "myvalue"},
    )

    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.http_get", return_value=MagicMock(text="abc123  file.txt")),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_multipart.assert_called_once_with(
            s3=s3_hook,
            s3_bucket="bucket",
            s3_key="prefix/custom_name.txt",
            url="http://example.com/file.txt",
            headers={"myheader": "myvalue"},
        )
        s3_hook.load_string.assert_not_called()


def test_download_via_local_configured_name_and_headers(s3_hook):
    download_config = DownloadConfig(
        url="http://example.com/file2.txt",
        use_direct_upload=False,
        md5_present=False,
        name="custom_name.txt",
        headers={"myheader": "myvalue"},
    )
    with (
        patch("dags.lib.domain.download.get_download_configs", return_value=[download_config]),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
    ):
        download(s3_hook, "bucket", "prefix", "source")
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="custom_name.txt", headers={"myheader": "myvalue"}
        )
        mock_check_md5.assert_not_called()
        mock_load.assert_called_once_with(
            s3=s3_hook,
            s3_bucket="bucket",
            dest_s3_key="prefix/custom_name.txt",
            local_file_name="custom_name.txt",
            md5_hash=None,
        )
