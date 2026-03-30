import pytest

from dags.lib.domain.model.download_config import DownloadConfig


def test_download_config_with_fixed_url():
    conf = DownloadConfig(
        download_url="http://example.com/file.txt",
        name="file.txt",
        headers={"Authorization": "Bearer token"},
        extract_members=None,
        use_direct_upload=False,
        md5_present=True,
    )
    assert conf.get_url("some_version") == "http://example.com/file.txt"
    assert conf.name == "file.txt"
    assert conf.headers == {"Authorization": "Bearer token"}
    assert conf.extract_members is None
    assert conf.use_direct_upload is False
    assert conf.md5_present is True


def test_download_config_with_dynamic_url():
    conf = DownloadConfig(
        download_url=lambda version: f"http://example.com/file_{version}.txt",
        name="file.txt",
        headers=None,
        extract_members=None,
        use_direct_upload=False,
        md5_present=False,
    )
    assert conf.get_url("1.1.0") == "http://example.com/file_1.1.0.txt"


def test_download_config_asserts_download_url_is_specified():
    with pytest.raises(ValueError, match="download_url must be provided as either a `str` or a `Callable`"):
        DownloadConfig(download_url=None, name="file.txt")


def test_download_config_asserts_on_direct_upload_and_extract_members():
    with pytest.raises(ValueError, match="direct upload does not support tar extract"):
        DownloadConfig(
            download_url="http://example.com/file.txt",
            name="file.txt",
            extract_members=["a.txt"],
            use_direct_upload=True,
        )
