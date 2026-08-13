from unittest.mock import Mock, patch

import pytest

_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads"


def test_ddd_get_latest_version(ddd_source_config, ddd_listing_html):
    mock_response = Mock(text=ddd_listing_html)
    with patch("opendatalake.lib.domain.source_configs.ddd.http_get", return_value=mock_response) as mock_http_get:
        version = ddd_source_config.get_latest_version()

    assert version == "2026_07_28"
    mock_http_get.assert_called_once_with(f"{_BASE_URL}/")


def test_ddd_get_latest_version_ignores_non_version_entries(ddd_source_config):
    # Only date-stamped directory hrefs count; loose text files must be ignored.
    html = """<html><body>
    <a href="Data_download_format_202501-202507.txt">fmt</a>
    <a href="G2PTermChanges202501.txt">changes</a>
    <a href="2025_12_28/">2025_12_28/</a>
    </body></html>"""
    with patch("opendatalake.lib.domain.source_configs.ddd.http_get", return_value=Mock(text=html)):
        assert ddd_source_config.get_latest_version() == "2025_12_28"


def test_ddd_get_latest_version_picks_most_recent(ddd_source_config):
    html = """<html><body>
    <a href="2026_07_28/">2026_07_28/</a>
    <a href="2025_02_28/">2025_02_28/</a>
    <a href="2026_01_28/">2026_01_28/</a>
    </body></html>"""
    with patch("opendatalake.lib.domain.source_configs.ddd.http_get", return_value=Mock(text=html)):
        assert ddd_source_config.get_latest_version() == "2026_07_28"


def test_ddd_get_latest_version_no_match_raises(ddd_source_config):
    html = """<html><body><a href="README.txt">README.txt</a></body></html>"""
    with (
        patch("opendatalake.lib.domain.source_configs.ddd.http_get", return_value=Mock(text=html)),
        pytest.raises(ValueError) as excinfo,
    ):
        ddd_source_config.get_latest_version()

    assert excinfo.value.args[0] == f"No G2P versions found at {_BASE_URL}/"


def test_ddd_get_latest_version_invalid_date_raises(ddd_source_config):
    html = """<html><body><a href="2026_13_45/">2026_13_45/</a></body></html>"""
    with (
        patch("opendatalake.lib.domain.source_configs.ddd.http_get", return_value=Mock(text=html)),
        pytest.raises(ValueError) as excinfo,
    ):
        ddd_source_config.get_latest_version()

    assert excinfo.value.args[0] == "G2P version '2026_13_45' is not a valid date (expected %Y_%m_%d)"


def test_ddd_download_url_built_from_version(ddd_source_config):
    url = ddd_source_config.download_configs[0].get_url("2026_07_28")
    assert url == f"{_BASE_URL}/2026_07_28/DDG2P_2026-07-28.csv.gz"


def test_ddd_local_file_name_is_version_independent(ddd_source_config):
    # `name` gives Spark a stable member name regardless of the version-stamped remote file.
    assert ddd_source_config.download_configs[0].name == "DDG2P.csv.gz"
