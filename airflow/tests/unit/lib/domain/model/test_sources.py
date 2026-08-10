from unittest.mock import Mock, patch

import pytest

from opendatalake.lib.domain.model.config import DownloadConfig, UpdateMode
from opendatalake.lib.domain.model.sources import (
    _get_source,
    get_all_source_ids,
    get_auto_update_source_ids,
    get_display_name,
    get_download_config_at_index,
    get_download_configs,
    get_latest_version,
    get_update_mode,
    is_auto_update,
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
    assert result
    for identifier in result:
        assert identifier.lower() == identifier
        assert not identifier.startswith("_")
        assert _get_source(identifier).value.update_mode == UpdateMode.AUTO


def test_source_id_derived_from_short_name():
    assert get_display_name("1000_genomes") == "1000 Genomes Project"
    assert "1000_genomes" not in get_auto_update_source_ids()


def test_get_all_source_ids_includes_manual():
    ids = get_all_source_ids()
    assert "1000_genomes" in ids
    assert set(get_auto_update_source_ids()).issubset(set(ids))
    assert len(ids) == len(set(ids)), "source ids must be unique"


def test_is_auto_update():
    assert is_auto_update("clinvar") is True
    assert is_auto_update("1000_genomes") is False


def test_get_update_mode():
    assert get_update_mode("clinvar") == "auto"
    assert get_update_mode("1000_genomes") == "manual"
    assert get_update_mode("gnomad_joint") == "manual"


def test_reverse_lookup_is_case_insensitive():
    for alias in ("1000_genomes", "1000_GENOMES"):
        assert get_display_name(alias) == "1000 Genomes Project"


def test_get_latest_version():
    mock_response = Mock()
    mock_response.text = "sometext clinvar_20240327.vcf"
    with patch("opendatalake.lib.domain.source_configs.clinvar.http_get", return_value=mock_response):
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
