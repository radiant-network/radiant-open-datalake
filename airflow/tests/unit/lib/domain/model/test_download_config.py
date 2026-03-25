import pytest

from dags.lib.domain.model.download_config import DownloadConfig


def test_download_config_with_url():
    conf = DownloadConfig(
        name="file.txt",
        url="http://example.com/file.txt",
        headers={"Authorization": "Bearer token"},
        extract_members=None,
        use_direct_upload=False,
        md5_present=True,
    )
    assert conf.get_url() == "http://example.com/file.txt"
    assert conf.name == "file.txt"
    assert conf.headers == {"Authorization": "Bearer token"}
    assert conf.extract_members is None
    assert conf.use_direct_upload is False
    assert conf.md5_present is True


def test_download_config_with_url_fn():
    conf = DownloadConfig(
        name="file.txt",
        url_fn=lambda: "http://example.com/file.txt",
        headers=None,
        extract_members=None,
        use_direct_upload=False,
        md5_present=False,
    )
    assert conf.get_url() == "http://example.com/file.txt"


def test_download_config_asserts_on_both_url_and_url_fn():
    with pytest.raises(AssertionError, match="Specify only one of url or url_fn"):
        DownloadConfig(
            name="file.txt", url="http://example.com/file.txt", url_fn=lambda: "http://example.com/file.txt"
        )


def test_download_config_asserts_on_neither_url_nor_url_fn():
    with pytest.raises(AssertionError, match="Either url or url_fn must be provided"):
        DownloadConfig(name="file.txt")


def test_download_config_asserts_on_direct_upload_and_extract_members():
    with pytest.raises(AssertionError, match="direct upload does not support tar extract"):
        DownloadConfig(
            name="file.txt", url="http://example.com/file.txt", extract_members=["a.txt"], use_direct_upload=True
        )
