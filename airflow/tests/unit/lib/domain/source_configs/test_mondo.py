from unittest.mock import Mock, patch

import pytest


def test_mondo_get_latest_version(mondo_source_config):
    mock_response = Mock(url="https://github.com/monarch-initiative/mondo/releases/tag/v2024-09-03")
    with patch("opendatalake.lib.domain.source_configs.mondo.http_get", return_value=mock_response) as mock_http_get:
        version = mondo_source_config.get_latest_version()

    assert version == "v2024-09-03"
    mock_http_get.assert_called_once_with("https://github.com/monarch-initiative/mondo/releases/latest")


def test_mondo_get_latest_version_no_match_raises(mondo_source_config):
    resolved_url = "https://github.com/monarch-initiative/mondo/releases"
    mock_response = Mock(url=resolved_url)
    with (
        patch("opendatalake.lib.domain.source_configs.mondo.http_get", return_value=mock_response),
        pytest.raises(ValueError) as excinfo,
    ):
        mondo_source_config.get_latest_version()

    assert excinfo.value.args[0] == f"Could not parse Mondo version from {resolved_url}"


def test_mondo_get_latest_version_invalid_date_raises(mondo_source_config):
    mock_response = Mock(url="https://github.com/monarch-initiative/mondo/releases/tag/v2024-13-99")
    with (
        patch("opendatalake.lib.domain.source_configs.mondo.http_get", return_value=mock_response),
        pytest.raises(ValueError) as excinfo,
    ):
        mondo_source_config.get_latest_version()

    assert excinfo.value.args[0] == "Mondo version 'v2024-13-99' is not a valid date (expected v%Y-%m-%d)"


def test_mondo_download_url_built_from_version(mondo_source_config):
    url = mondo_source_config.download_configs[0].get_url("v2024-09-03")
    assert url == "https://github.com/monarch-initiative/mondo/releases/download/v2024-09-03/mondo-base.obo"
