from unittest.mock import Mock, patch

import pytest

from dags.lib.domain.model.config import DownloadConfig, UpdateMode
from dags.lib.domain.model.sources import (
    _Source,
    get_auto_update_source_ids,
    get_download_config_at_index,
    get_download_configs,
    get_latest_version,
)


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
    with patch("dags.lib.domain.source_configs.clinvar.http_get", return_value=mock_response):
        assert get_latest_version("clinvar") == "20240327"
        assert get_latest_version("Clinvar") == "20240327"


def test_get_download_config_at_index_valid():
    config = get_download_config_at_index("clinvar", 0)
    assert isinstance(config, DownloadConfig)
    assert config.label == "vcf"


def test_get_download_config_at_index_invalid():
    # Assuming 'clinvar' is a valid source and has only one config (index 0)
    with pytest.raises(IndexError) as exc:
        get_download_config_at_index("clinvar", 1)
    assert "invalid for 'clinvar'" in str(exc.value)

    with pytest.raises(IndexError) as exc:
        get_download_config_at_index("clinvar", -1)
    assert "invalid for 'clinvar'" in str(exc.value)


def test_get_download_config_at_index_invalid_source():
    with pytest.raises(KeyError):
        get_download_config_at_index("not_a_source", 0)
