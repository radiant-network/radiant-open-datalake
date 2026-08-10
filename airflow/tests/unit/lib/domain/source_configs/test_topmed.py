import os
from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowException

from opendatalake.lib.domain.model.config import UpdateMode
from opendatalake.lib.domain.source_configs.topmed import (
    COOKIE_ARN_ENV_VAR,
    COOKIE_ENV_VAR,
    TopMedBravoConfig,
    _fetch_cookie_from_secrets_manager,
)

CHROMOSOMES = [str(c) for c in range(1, 23)] + ["X"]
LINK_API_ROOT = "https://api.bravo.sph.umich.edu/ui/link?chrom=chr"


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


def test_topmed_declares_the_cookie_secret(topmed_source_config):
    for config in topmed_source_config.download_configs:
        assert config.secret_env_vars == ((COOKIE_ENV_VAR, COOKIE_ARN_ENV_VAR),)


def test_topmed_attaches_the_cookie_header(topmed_source_config):
    # The cookie is resolved lazily at download time from the env (conftest seeds it for the test run).
    for config in topmed_source_config.download_configs:
        assert "Cookie" in config.get_headers()


def test_topmed_is_manually_updated_and_imports_via_the_topmed_command(topmed_source_config):
    assert topmed_source_config.update_mode is UpdateMode.MANUAL
    assert topmed_source_config.import_config.spark_command == "topmed_bravo"


def test_config_from_env_prefers_the_plaintext_cookie_over_secrets_manager():
    TopMedBravoConfig.from_env.cache_clear()
    try:
        with (
            patch.dict(os.environ, {COOKIE_ENV_VAR: "plaintext-cookie"}),
            patch(
                "opendatalake.lib.domain.source_configs.topmed._fetch_cookie_from_secrets_manager"
            ) as fetch,
        ):
            assert TopMedBravoConfig.from_env().cookie == "plaintext-cookie"
            fetch.assert_not_called()
    finally:
        TopMedBravoConfig.from_env.cache_clear()


def test_config_from_env_fetches_from_secrets_manager_when_no_plaintext_cookie():
    TopMedBravoConfig.from_env.cache_clear()
    try:
        # clear=True drops conftest's seeded plaintext cookie so the ARN path is exercised.
        with (
            patch.dict(os.environ, {COOKIE_ARN_ENV_VAR: "arn:aws:secretsmanager:...:secret:ck"}, clear=True),
            patch(
                "opendatalake.lib.domain.source_configs.topmed._fetch_cookie_from_secrets_manager",
                return_value="sm-cookie",
            ) as fetch,
        ):
            assert TopMedBravoConfig.from_env().cookie == "sm-cookie"
            fetch.assert_called_once_with("arn:aws:secretsmanager:...:secret:ck")
    finally:
        TopMedBravoConfig.from_env.cache_clear()


def test_fetch_cookie_from_secrets_manager_returns_stripped_secret_string():
    with patch("opendatalake.lib.domain.source_configs.topmed.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = "  sm-cookie\n"
        assert _fetch_cookie_from_secrets_manager("arn:aws:secretsmanager:...:secret:ck") == "sm-cookie"
        hook_cls.assert_called_once_with(aws_conn_id="aws_default")


def test_fetch_cookie_from_secrets_manager_decodes_binary_secret():
    with patch("opendatalake.lib.domain.source_configs.topmed.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = b"sm-cookie"
        assert _fetch_cookie_from_secrets_manager("arn:aws:secretsmanager:...:secret:ck") == "sm-cookie"


def test_fetch_cookie_from_secrets_manager_skips_aws_without_an_arn():
    with patch("opendatalake.lib.domain.source_configs.topmed.SecretsManagerHook") as hook_cls:
        assert _fetch_cookie_from_secrets_manager("") == ""
        hook_cls.assert_not_called()


def test_config_auth_headers_uses_the_cookie_header():
    assert TopMedBravoConfig(cookie="ck").auth_headers() == {"Cookie": "ck"}


def test_config_missing_cookie_is_detected():
    assert TopMedBravoConfig(cookie="").missing() is True
    assert TopMedBravoConfig(cookie="ck").missing() is False


def test_config_auth_headers_raises_when_cookie_absent():
    with pytest.raises(AirflowException, match=COOKIE_ENV_VAR):
        TopMedBravoConfig(cookie="").auth_headers()
