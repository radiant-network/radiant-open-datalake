from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowException

from opendatalake.lib.domain.model.config import UpdateMode
from opendatalake.lib.domain.source_configs import topmed
from opendatalake.lib.domain.source_configs.topmed import set_headers

CHROMOSOMES = [str(c) for c in range(1, 23)] + ["X"]
LINK_API_ROOT = "https://api.bravo.sph.umich.edu/ui/link?chrom=chr"


@pytest.fixture(autouse=True)
def _reset_headers():
    set_headers({})
    yield
    set_headers({})


def test_topmed_declares_one_config_per_chromosome(topmed_source_config):
    labels = [c.label for c in topmed_source_config.download_configs]
    assert labels == [f"chr{c}" for c in CHROMOSOMES]


def test_topmed_names_match_the_spark_raw_glob(topmed_source_config):
    # Spark reads /topmed_bravo/<version>/bravo-dbsnp-*.vcf.gz — every file must match that glob.
    names = [c.name for c in topmed_source_config.download_configs]
    assert names[0] == "bravo-dbsnp-chr1.vcf.gz"
    assert names[-1] == "bravo-dbsnp-chrX.vcf.gz"
    assert all(n.startswith("bravo-dbsnp-chr") and n.endswith(".vcf.gz") for n in names)


def test_topmed_resolves_the_signed_url_per_chromosome(topmed_source_config):
    set_headers({"Cookie": "ck"})

    def fake_http_get(url, headers=None):
        chrom = url.rsplit("chrom=", 1)[-1]
        return Mock(**{"json.return_value": {"url": f"https://signed/{chrom}.vcf.gz"}})

    with patch(
        "opendatalake.lib.domain.source_configs.topmed.http_get", side_effect=fake_http_get
    ) as mock_http_get:
        # The version argument is ignored; the URL comes from the link API per chromosome.
        urls = {c.label: c.get_url("ignored") for c in topmed_source_config.download_configs}

    assert urls["chr1"] == "https://signed/chr1.vcf.gz"
    assert urls["chrX"] == "https://signed/chrX.vcf.gz"
    # The link API is queried with the cookie header.
    for call in mock_http_get.call_args_list:
        headers = call.args[1] if len(call.args) > 1 else call.kwargs.get("headers", {})
        assert "Cookie" in headers


def test_topmed_streams_every_file_and_declares_no_md5(topmed_source_config):
    # Whole-genome per-chromosome VCFs are large — stream them; BRAVO ships no checksum.
    for config in topmed_source_config.download_configs:
        assert config.use_stream_upload is True
        assert config.md5_present is False


def test_topmed_declares_set_headers_hook(topmed_source_config):
    for config in topmed_source_config.download_configs:
        assert config.set_headers is topmed.set_headers


def test_topmed_attaches_the_cookie_header(topmed_source_config):
    set_headers({"Cookie": "ck"})
    for config in topmed_source_config.download_configs:
        assert config.get_headers() == {"Cookie": "ck"}


def test_topmed_is_manually_updated_and_imports_via_the_topmed_command(topmed_source_config):
    assert topmed_source_config.update_mode is UpdateMode.MANUAL
    assert topmed_source_config.import_config.spark_command == "topmed_bravo"


def test_set_headers_updates_module_state():
    set_headers({"Cookie": "a-cookie"})
    assert topmed._headers == {"Cookie": "a-cookie"}


def test_auth_headers_uses_the_set_headers():
    set_headers({"Cookie": "ck"})
    assert topmed._auth_headers() == {"Cookie": "ck"}


def test_auth_headers_raises_when_headers_not_set():
    with pytest.raises(AirflowException, match="headers"):
        topmed._auth_headers()
