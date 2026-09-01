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
    get_import_config,
    get_latest_version,
    get_update_mode,
    is_auto_update,
    requires_download_url,
)


def test_dbnsfp_is_manual_url_source():
    assert get_update_mode("dbnsfp") == "manual"
    assert is_auto_update("dbnsfp") is False
    assert requires_download_url("dbnsfp") is True
    # fixed-URL sources do not take a runtime URL
    assert requires_download_url("clinvar") is False


def test_dbnsfp_download_config_stream_unzips_variant_members():
    (conf,) = get_download_configs("dbnsfp")
    assert conf.url_from_param is True
    assert conf.use_stream_unzip is True
    assert conf.download_url is None
    assert conf.member_pattern == "*_variant.chr*.gz"


def test_dbnsfp_import_config():
    import_config = get_import_config("dbnsfp")
    assert import_config.spark_command == "dbnsfp"
    assert import_config.waiter_max_attempts == 960
    assert import_config.spark_conf == {
        "spark.dynamicAllocation.maxExecutors": "16",
        "spark.dynamicAllocation.initialExecutors": "5",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "2g",
        "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
        "spark.emr-serverless.executor.disk": "100G",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }


def test_orphanet_is_auto_source():
    assert get_update_mode("orphanet") == "auto"
    assert is_auto_update("orphanet") is True
    assert requires_download_url("orphanet") is False
    assert "orphanet" in get_auto_update_source_ids()


def test_orphanet_download_configs_are_two_fixed_url_xml_files():
    configs = get_download_configs("orphanet")
    assert [c.name for c in configs] == ["en_product6.xml", "en_product9_ages.xml"]
    assert all(c.url_from_param is False for c in configs)
    assert configs[0].get_url("") == "https://www.orphadata.com/data/xml/en_product6.xml"
    assert configs[1].get_url("") == "https://www.orphadata.com/data/xml/en_product9_ages.xml"


def test_orphanet_import_config():
    assert get_import_config("orphanet").spark_command == "orphanet"


def test_ddd_is_auto_source():
    assert get_update_mode("ddd") == "auto"
    assert is_auto_update("ddd") is True
    assert requires_download_url("ddd") is False
    assert "ddd" in get_auto_update_source_ids()


def test_ddd_download_config():
    (conf,) = get_download_configs("ddd")
    assert conf.md5_present is True
    assert conf.name == "DDG2P.csv.gz"
    assert conf.get_url("2026_07_28") == (
        "https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/2026_07_28/DDG2P_2026-07-28.csv.gz"
    )


def test_ddd_import_config():
    assert get_import_config("ddd").spark_command == "ddd"


def test_omim_is_manual_source():
    assert get_update_mode("omim") == "manual"
    assert is_auto_update("omim") is False
    assert requires_download_url("omim") is False


def test_omim_download_config_declares_the_key_secret():
    (conf,) = get_download_configs("omim")
    assert conf.name == "genemap2.txt"
    # the container reads the injected key from this env var; the ARN env var names its Secrets Manager ARN
    assert conf.secret_env_vars == (("OPENDATALAKE_OMIM_DOWNLOAD_KEY", "OPENDATALAKE_OMIM_DOWNLOAD_KEY_ARN"),)


def test_omim_import_config():
    assert get_import_config("omim").spark_command == "omim"


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
