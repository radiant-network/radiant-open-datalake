from unittest.mock import Mock, patch

import pytest


def test_clinvar_get_latest_version(clinvar_source_config):
    mock_response = Mock()
    mock_response.text = "some text clinvar_20240327.vcf more text"
    with patch("dags.lib.domain.source_configs.clinvar.http_get", return_value=mock_response) as mock_http_get:
        version = clinvar_source_config.get_latest_version()
        assert version == "20240327"
        mock_http_get.assert_called_once_with("https://example.com/clinvar.md5")


def test_clinvar_get_latest_version_no_match_raises(clinvar_source_config):
    mock_response = Mock(text="no version pattern here")
    with (
        patch("dags.lib.domain.source_configs.clinvar.http_get", return_value=mock_response),
        pytest.raises(ValueError) as excinfo,
    ):
        clinvar_source_config.get_latest_version()

    assert excinfo.value.args[0] == "Could not parse ClinVar version from https://example.com/clinvar.md5"


