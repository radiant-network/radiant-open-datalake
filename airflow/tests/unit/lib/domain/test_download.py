from unittest.mock import ANY, MagicMock, call, patch

from dags.lib.config import raw_datalake_bucket
from dags.lib.domain.download import S3Downloader
from dags.lib.domain.model.config import DownloadConfig


def test_direct_upload(s3_hook):
    download_config = DownloadConfig(download_url="http://example.com/file.txt", use_direct_upload=True)

    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  file.txt"),
        ),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )

        downloader.direct_upload()
        mock_multipart.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/file.txt",
            url="http://example.com/file.txt",
            headers={},
        )
        s3_hook.load_string.assert_not_called()


def test_direct_upload_with_md5(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/file.txt", use_direct_upload=True, md5_present=True
    )
    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  file.txt"),
        ),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.direct_upload()
        mock_multipart.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/file.txt",
            url="http://example.com/file.txt",
            headers={},
        )
        s3_hook.load_string.assert_called_once_with(
            "938c2cc0dcc05f2b68c4287040cfcf71", "prefix/file.txt.md5", raw_datalake_bucket, replace=True
        )


def test_direct_upload_with_configured_name_and_headers(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/file.txt",
        use_direct_upload=True,
        md5_present=False,
        name="custom_name.txt",
        headers={"myheader": "myvalue"},
    )

    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  file.txt"),
        ),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.direct_upload()
        mock_multipart.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/custom_name.txt",
            url="http://example.com/file.txt",
            headers={"myheader": "myvalue"},
        )
        s3_hook.load_string.assert_not_called()


def test_direct_upload_with_dynamic_url(s3_hook):
    download_config = DownloadConfig(
        download_url=lambda version: f"http://example.com/file_{version}.txt",
        use_direct_upload=True,
        md5_present=False,
    )

    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  file_1.1.0.txt"),
        ),
        patch("dags.lib.domain.download.multipart_upload_with_resume") as mock_multipart,
    ):
        downloader = S3Downloader(s3=s3_hook, s3_prefix="prefix", version="1.1.0", download_conf=download_config)
        downloader.direct_upload()
        mock_multipart.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/file_1.1.0.txt",
            url="http://example.com/file_1.1.0.txt",
            headers={},
        )
        s3_hook.load_string.assert_not_called()


def test_upload_via_local_copy(s3_hook):
    download_config = DownloadConfig(download_url="http://example.com/file2.txt", use_direct_upload=False)

    with (
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile") as tarfile_mock,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.upload_via_local_copy()
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="file2.txt", headers={}
        )
        mock_check_md5.assert_not_called()
        mock_load.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/file2.txt",
            local_file_name="file2.txt",
            md5_hash=None,
        )
        tarfile_mock.open.assert_not_called()


def test_upload_via_local_copy_with_md5(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/file2.txt", use_direct_upload=False, md5_present=True
    )
    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  file2.txt"),
        ),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile") as tarfile_mock,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.upload_via_local_copy()
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="file2.txt", headers={}
        )
        mock_check_md5.assert_called_once_with("file2.txt", "938c2cc0dcc05f2b68c4287040cfcf71")
        mock_load.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/file2.txt",
            local_file_name="file2.txt",
            md5_hash="938c2cc0dcc05f2b68c4287040cfcf71",
        )
        tarfile_mock.open.assert_not_called()


def test_upload_via_local_copy_with_extract_members_no_md5(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/archive.tar.gz",
        use_direct_upload=False,
        md5_present=False,
        extract_members=["file1.txt", "file2.txt"],
    )
    with (
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile.open") as mock_tarfile_open,
    ):
        mock_tar = MagicMock()
        mock_tarfile_open.return_value.__enter__.return_value = mock_tar
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.upload_via_local_copy()
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/archive.tar.gz", dest_file_name="archive.tar.gz", headers={}
        )
        mock_tar.extractall.assert_called_once_with(filter=ANY)
        mock_load.assert_has_calls(
            [
                call(
                    s3=s3_hook,
                    s3_bucket=raw_datalake_bucket,
                    s3_key="prefix/file1.txt",
                    local_file_name="file1.txt",
                    md5_hash=None,
                ),
                call(
                    s3=s3_hook,
                    s3_bucket=raw_datalake_bucket,
                    s3_key="prefix/file2.txt",
                    local_file_name="file2.txt",
                    md5_hash=None,
                ),
            ],
            any_order=True,
        )


def test_upload_via_local_copy_with_extract_members_with_md5(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/archive.tar.gz",
        use_direct_upload=False,
        md5_present=True,
        extract_members=["file1.txt", "file2.txt"],
    )
    with (
        patch(
            "dags.lib.domain.download.http_get",
            return_value=MagicMock(text="938c2cc0dcc05f2b68c4287040cfcf71  archive.tar"),
        ),
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.compute_file_md5", side_effect=["abcde", "fghij"]),
        patch("dags.lib.domain.download.check_md5"),
        patch("dags.lib.domain.download.load_file") as mock_load,
        patch("dags.lib.domain.download.tarfile.open") as mock_tarfile_open,
    ):
        mock_tar = MagicMock()
        mock_tarfile_open.return_value.__enter__.return_value = mock_tar
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.upload_via_local_copy()

        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/archive.tar.gz", dest_file_name="archive.tar.gz", headers={}
        )
        mock_tar.extractall.assert_called_once_with(filter=ANY)
        mock_load.assert_has_calls(
            [
                call(
                    s3=s3_hook,
                    s3_bucket=raw_datalake_bucket,
                    s3_key="prefix/file1.txt",
                    local_file_name="file1.txt",
                    md5_hash="abcde",
                ),
                call(
                    s3=s3_hook,
                    s3_bucket=raw_datalake_bucket,
                    s3_key="prefix/file2.txt",
                    local_file_name="file2.txt",
                    md5_hash="fghij",
                ),
            ],
            any_order=True,
        )


def test_upload_via_local_copy_with_configured_name_and_headers(s3_hook):
    download_config = DownloadConfig(
        download_url="http://example.com/file2.txt",
        use_direct_upload=False,
        md5_present=False,
        name="custom_name.txt",
        headers={"myheader": "myvalue"},
    )
    with (
        patch("dags.lib.domain.download.stream_download_file") as mock_stream_download_file,
        patch("dags.lib.domain.download.check_md5") as mock_check_md5,
        patch("dags.lib.domain.download.load_file") as mock_load,
    ):
        downloader = S3Downloader(
            s3=s3_hook, s3_prefix="prefix", version="some_version", download_conf=download_config
        )
        downloader.upload_via_local_copy()
        mock_stream_download_file.assert_called_once_with(
            url="http://example.com/file2.txt", dest_file_name="custom_name.txt", headers={"myheader": "myvalue"}
        )
        mock_check_md5.assert_not_called()
        mock_load.assert_called_once_with(
            s3=s3_hook,
            s3_bucket=raw_datalake_bucket,
            s3_key="prefix/custom_name.txt",
            local_file_name="custom_name.txt",
            md5_hash=None,
        )
