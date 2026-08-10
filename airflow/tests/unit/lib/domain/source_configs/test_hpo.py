from unittest.mock import Mock, patch

import pytest


def test_hpo_get_latest_version(hpo_source_config):
    mock_response = Mock(url="https://github.com/obophenotype/human-phenotype-ontology/releases/tag/v2024-08-13")
    with patch("opendatalake.lib.domain.source_configs.hpo.http_get", return_value=mock_response) as mock_http_get:
        version = hpo_source_config.get_latest_version()

    assert version == "v2024-08-13"
    mock_http_get.assert_called_once_with("https://github.com/obophenotype/human-phenotype-ontology/releases/latest")


def test_hpo_get_latest_version_no_match_raises(hpo_source_config):
    resolved_url = "https://github.com/obophenotype/human-phenotype-ontology/releases"
    mock_response = Mock(url=resolved_url)
    with (
        patch("opendatalake.lib.domain.source_configs.hpo.http_get", return_value=mock_response),
        pytest.raises(ValueError) as excinfo,
    ):
        hpo_source_config.get_latest_version()

    assert excinfo.value.args[0] == f"Could not parse HPO version from {resolved_url}"


def test_hpo_get_latest_version_invalid_date_raises(hpo_source_config):
    # tag matches the pattern but is not a real calendar date
    mock_response = Mock(url="https://github.com/obophenotype/human-phenotype-ontology/releases/tag/v2026-13-99")
    with (
        patch("opendatalake.lib.domain.source_configs.hpo.http_get", return_value=mock_response),
        pytest.raises(ValueError) as excinfo,
    ):
        hpo_source_config.get_latest_version()

    assert excinfo.value.args[0] == "HPO version 'v2026-13-99' is not a valid date (expected v%Y-%m-%d)"


def test_hpo_download_url_built_from_version(hpo_source_config):
    url = hpo_source_config.download_configs[0].get_url("v2024-08-13")
    assert url == "https://github.com/obophenotype/human-phenotype-ontology/releases/download/v2024-08-13/hp.obo"
