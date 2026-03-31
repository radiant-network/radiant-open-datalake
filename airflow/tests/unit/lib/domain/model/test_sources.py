import pytest

from dags.lib.domain.model.sources import get_download_configs


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
