from unittest.mock import Mock, patch

import pytest

from opendatalake.lib.domain.model.config import DownloadConfig, UpdateMode
from opendatalake.lib.domain.source_configs import OrphanetSourceConfig

_BASE_URL = "https://www.orphadata.com/data/xml"

_HEADER = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    '<JDBOR date="2021-01-01 04:39:55" version="1.3.7 / 4.1.7 [2020-12-03] (orientdb version)" '
    'copyright="Orphanet (c) 2021">\n'
    "    <Availability>\n"
)


@pytest.fixture
def orphanet_source_config() -> OrphanetSourceConfig:
    return OrphanetSourceConfig(
        short_name="orphanet",
        display_name="Orphanet",
        website="https://www.orphadata.com/",
        download_configs=[
            DownloadConfig(download_url=f"{_BASE_URL}/en_product6.xml", name="en_product6.xml", label="xml"),
            DownloadConfig(download_url=f"{_BASE_URL}/en_product9_ages.xml", name="en_product9_ages.xml", label="xml"),
        ],
        update_mode=UpdateMode.AUTO,
    )


def test_get_latest_version_parses_jdbor_version_attribute(orphanet_source_config):
    with patch(
        "opendatalake.lib.domain.source_configs.orphanet.http_get", return_value=Mock(text=_HEADER)
    ) as mock_http_get:
        version = orphanet_source_config.get_latest_version()

    # version numbers + bracketed date, made path/branch-safe; note the embedded date (2020-12-03) is used.
    assert version == "1.3.7_4.1.7_2020-12-03"
    # Reads the genes product header via a bounded range request (not the whole file).
    (called_url,), kwargs = mock_http_get.call_args
    assert called_url == f"{_BASE_URL}/en_product6.xml"
    assert kwargs["headers"]["Range"].startswith("bytes=0-")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1.3.7 / 4.1.7 [2020-12-03] (orientdb version)", "1.3.7_4.1.7_2020-12-03"),
        ("2.0 / 5.0 [2024-06-01]", "2.0_5.0_2024-06-01"),  # no trailing note
        ("1.3.7 / 4.1.7 [2020-12-03] (orientdb version)  ", "1.3.7_4.1.7_2020-12-03"),  # trailing whitespace
    ],
)
def test_normalize_version_keeps_numbers_and_date(raw, expected):
    assert OrphanetSourceConfig._normalize_version(raw) == expected


def test_get_latest_version_raises_without_version_attribute(orphanet_source_config):
    body = '<?xml version="1.0"?>\n<JDBOR date="2021-01-01 04:39:55"/>'
    with (
        patch("opendatalake.lib.domain.source_configs.orphanet.http_get", return_value=Mock(text=body)),
        pytest.raises(ValueError, match="Could not parse the Orphanet <JDBOR> version attribute"),
    ):
        orphanet_source_config.get_latest_version()


def test_download_urls_are_fixed(orphanet_source_config):
    urls = [c.get_url("") for c in orphanet_source_config.download_configs]
    assert urls == [f"{_BASE_URL}/en_product6.xml", f"{_BASE_URL}/en_product9_ages.xml"]
