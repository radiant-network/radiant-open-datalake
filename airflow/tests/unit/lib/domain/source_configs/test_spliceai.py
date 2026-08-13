import os
from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowException

from opendatalake.lib.domain.model.config import UpdateMode
from opendatalake.lib.domain.source_configs.spliceai import (
    ACCESS_TOKEN_ARN_ENV_VAR,
    ACCESS_TOKEN_ENV_VAR,
    SpliceAiConfig,
    _fetch_token_from_secrets_manager,
    _sanitize_etag,
)

VARIANT_TYPES = ("snv", "indel")
API_ROOT = "https://api.basespace.illumina.com/v2/files"
FILE_IDS = {
    "snv": {"vcf": "16525380715", "tbi": "16525505189"},
    "indel": {"vcf": "16525003580", "tbi": "16525276839"},
}


def test_spliceai_declares_a_vcf_and_an_index_per_variant_type(spliceai_source_config):
    labels = [c.label for c in spliceai_source_config.download_configs]
    assert labels == [f"{vt}_{kind}" for vt in VARIANT_TYPES for kind in ("vcf", "tbi")]


def test_spliceai_names_match_the_spark_raw_pattern(spliceai_source_config):
    names = {c.label: c.name for c in spliceai_source_config.download_configs}
    assert names["snv_vcf"] == "spliceai_scores.raw.snv.hg38.vcf.gz"
    assert names["indel_vcf"] == "spliceai_scores.raw.indel.hg38.vcf.gz"
    assert names["snv_tbi"] == "spliceai_scores.raw.snv.hg38.vcf.gz.tbi"


def test_spliceai_downloads_from_the_file_content_endpoint(spliceai_source_config):
    urls = {c.label: c.get_url("any") for c in spliceai_source_config.download_configs}
    assert urls["snv_vcf"] == f"{API_ROOT}/{FILE_IDS['snv']['vcf']}/content"
    assert urls["indel_tbi"] == f"{API_ROOT}/{FILE_IDS['indel']['tbi']}/content"


def test_spliceai_streams_vcfs_but_not_indexes(spliceai_source_config):
    # The score VCFs are large — stream them; the tiny tabix indexes go through the local-copy path.
    for config in spliceai_source_config.download_configs:
        assert config.use_stream_upload is config.label.endswith("_vcf")


def test_spliceai_declares_no_md5(spliceai_source_config):
    assert all(c.md5_present is False for c in spliceai_source_config.download_configs)


def test_spliceai_declares_the_token_secret(spliceai_source_config):
    # So the ECS local-copy operator injects the token via secrets/valueFrom (the ARN), not plaintext.
    for config in spliceai_source_config.download_configs:
        assert config.secret_env_vars == ((ACCESS_TOKEN_ENV_VAR, ACCESS_TOKEN_ARN_ENV_VAR),)


def test_spliceai_attaches_auth_headers(spliceai_source_config):
    # The token is resolved lazily at download time from the env (conftest seeds it for the test run).
    for config in spliceai_source_config.download_configs:
        assert "x-access-token" in config.get_headers()


def test_spliceai_is_manually_updated_and_imports_via_the_spliceai_command(spliceai_source_config):
    assert spliceai_source_config.update_mode is UpdateMode.MANUAL
    assert spliceai_source_config.import_config.spark_command == "spliceai"


def test_spliceai_version_joins_both_vcf_etags(spliceai_source_config):
    responses = {
        FILE_IDS["snv"]["vcf"]: Mock(**{"json.return_value": {"ETag": '"snv-etag"'}}),
        FILE_IDS["indel"]["vcf"]: Mock(**{"json.return_value": {"ETag": 'W/"indel-etag"'}}),
    }

    def fake_http_get(url, headers=None):
        file_id = url.rsplit("/", 1)[-1]
        return responses[file_id]

    with patch("opendatalake.lib.domain.source_configs.spliceai.http_get", side_effect=fake_http_get) as mock_http_get:
        assert spliceai_source_config.get_latest_version() == "snv-etag_indel-etag"

    # One metadata call per VCF, and the auth header is passed.
    assert mock_http_get.call_count == 2
    for call in mock_http_get.call_args_list:
        assert "x-access-token" in call.kwargs.get("headers", {}) or "x-access-token" in call.args[1]


def test_config_from_env_prefers_the_plaintext_token_over_secrets_manager():
    with (
        patch.dict(os.environ, {ACCESS_TOKEN_ENV_VAR: "secret-token"}),
        patch("opendatalake.lib.domain.source_configs.spliceai._fetch_token_from_secrets_manager") as fetch,
    ):
        assert SpliceAiConfig.from_env().access_token == "secret-token"
        fetch.assert_not_called()


def test_config_from_env_fetches_from_secrets_manager_when_no_plaintext_token():
    with (
        patch.dict(os.environ, {ACCESS_TOKEN_ARN_ENV_VAR: "arn:aws:secretsmanager:...:secret:tok"}, clear=True),
        patch(
            "opendatalake.lib.domain.source_configs.spliceai._fetch_token_from_secrets_manager",
            return_value="sm-token",
        ) as fetch,
    ):
        assert SpliceAiConfig.from_env().access_token == "sm-token"
        fetch.assert_called_once_with("arn:aws:secretsmanager:...:secret:tok")


def test_fetch_token_from_secrets_manager_returns_stripped_secret_string():
    with patch("opendatalake.lib.domain.source_configs.spliceai.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = "  sm-token\n"
        assert _fetch_token_from_secrets_manager("arn:aws:secretsmanager:...:secret:tok") == "sm-token"
        hook_cls.assert_called_once_with(aws_conn_id="aws_default")
        hook_cls.return_value.get_secret.assert_called_once_with("arn:aws:secretsmanager:...:secret:tok")


def test_fetch_token_from_secrets_manager_decodes_binary_secret():
    with patch("opendatalake.lib.domain.source_configs.spliceai.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = b"sm-token"
        assert _fetch_token_from_secrets_manager("arn:aws:secretsmanager:...:secret:tok") == "sm-token"


def test_fetch_token_from_secrets_manager_skips_aws_without_an_arn():
    with patch("opendatalake.lib.domain.source_configs.spliceai.SecretsManagerHook") as hook_cls:
        assert _fetch_token_from_secrets_manager("") == ""
        hook_cls.assert_not_called()


def test_config_auth_headers_uses_the_x_access_token_header():
    assert SpliceAiConfig(access_token="secret-token").auth_headers() == {"x-access-token": "secret-token"}


def test_config_missing_token_is_detected():
    assert SpliceAiConfig(access_token="").missing() is True
    assert SpliceAiConfig(access_token="secret-token").missing() is False


def test_config_auth_headers_raises_when_token_absent():
    with pytest.raises(AirflowException, match=ACCESS_TOKEN_ENV_VAR):
        SpliceAiConfig(access_token="").auth_headers()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("d41d8cd98f00b204e9800998ecf8427e", "d41d8cd98f00b204e9800998ecf8427e"),
        ('"d41d8cd98f00b204e9800998ecf8427e"', "d41d8cd98f00b204e9800998ecf8427e"),  # RFC 7232 quoted
        ('W/"abc123-2"', "abc123-2"),  # weak validator + multipart ETag
        ("  abc123  ", "abc123"),  # surrounding whitespace
    ],
)
def test_sanitize_etag_strips_quotes_weak_prefix_and_whitespace(raw, expected):
    assert _sanitize_etag(raw) == expected


@pytest.mark.parametrize("bad", ['"abc/def"', 'a"b', "abc def", "", '""'])
def test_sanitize_etag_rejects_unsafe_values(bad):
    with pytest.raises(AirflowException, match="not usable as a version"):
        _sanitize_etag(bad)
