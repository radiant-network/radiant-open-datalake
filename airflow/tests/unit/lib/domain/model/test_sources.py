from unittest.mock import Mock, patch

import pytest

from dags.lib.domain.model.config import UpdateMode
from dags.lib.domain.model.sources import _Source, get_auto_update_source_ids, get_download_configs, get_latest_version


def test_get_download_configs_with_string_lowercase():
    configs = get_download_configs("clinvar")
    assert isinstance(configs, list)
    assert configs
    assert configs[0].download_url == "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"


def test_get_download_configs_with_string_uppercase():
    configs = get_download_configs("CLINVAR")
    assert isinstance(configs, list)
    assert configs
    assert configs[0].download_url == "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"


def test_get_download_configs_invalid_string():
    with pytest.raises(KeyError):
        get_download_configs("not_a_source")


def test_get_auto_update_source_ids():
    result = get_auto_update_source_ids()
    for identifier in result:
        assert identifier.lower() == identifier
        assert _Source[identifier.upper()].value.update_mode == UpdateMode.AUTO


def test_get_latest_version():
    mock_response = Mock()
    mock_response.text = "sometext clinvar_20240327.vcf"
    with patch("dags.lib.domain.sources_impl.http_get", return_value=mock_response):
        assert get_latest_version("clinvar") == "20240327"
        assert get_latest_version("Clinvar") == "20240327"
