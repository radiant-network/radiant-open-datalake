import os
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException

from opendatalake.lib.domain.model.config import UpdateMode
from opendatalake.lib.domain.source_configs.omim import (
    DOWNLOAD_KEY_ARN_ENV_VAR,
    DOWNLOAD_KEY_ENV_VAR,
    OmimConfig,
    _fetch_key_from_secrets_manager,
)


def test_omim_declares_a_single_genemap2_tsv(omim_source_config):
    (config,) = omim_source_config.download_configs
    assert config.name == "genemap2.txt"
    assert config.label == "tsv"
    assert config.use_stream_upload is False
    assert config.use_stream_unzip is False


def test_omim_declares_the_download_key_secret(omim_source_config):
    (config,) = omim_source_config.download_configs
    assert config.secret_env_vars == (DOWNLOAD_KEY_ENV_VAR,)
    assert config.secret_arn_env_vars == (DOWNLOAD_KEY_ARN_ENV_VAR,)


def test_omim_builds_the_url_from_the_env_key(omim_source_config):
    (config,) = omim_source_config.download_configs
    with patch.dict(os.environ, {DOWNLOAD_KEY_ENV_VAR: "secret-key"}):
        assert config.get_url("any") == "https://data.omim.org/downloads/secret-key/genemap2.txt"


def test_omim_url_raises_when_key_absent(omim_source_config):
    (config,) = omim_source_config.download_configs
    with (
        patch.dict(os.environ, {}, clear=True),
        patch("opendatalake.lib.domain.source_configs.omim._fetch_key_from_secrets_manager", return_value=""),
        pytest.raises(AirflowException, match=DOWNLOAD_KEY_ARN_ENV_VAR),
    ):
        config.get_url("any")


def test_omim_is_manually_updated_and_imports_via_the_omim_command(omim_source_config):
    assert omim_source_config.update_mode is UpdateMode.MANUAL
    assert omim_source_config.import_config.spark_command == "omim"


def test_omim_has_no_discoverable_version(omim_source_config):
    with pytest.raises(NotImplementedError):
        omim_source_config.get_latest_version()


def test_config_from_env_prefers_the_plaintext_key_over_secrets_manager():
    with (
        patch.dict(os.environ, {DOWNLOAD_KEY_ENV_VAR: "env-key"}),
        patch("opendatalake.lib.domain.source_configs.omim._fetch_key_from_secrets_manager") as fetch,
    ):
        assert OmimConfig.from_env().download_key == "env-key"
        fetch.assert_not_called()


def test_config_from_env_fetches_from_secrets_manager_when_no_plaintext_key():
    with (
        patch.dict(os.environ, {DOWNLOAD_KEY_ARN_ENV_VAR: "arn:aws:secretsmanager:...:secret:omim"}, clear=True),
        patch(
            "opendatalake.lib.domain.source_configs.omim._fetch_key_from_secrets_manager",
            return_value="sm-key",
        ) as fetch,
    ):
        assert OmimConfig.from_env().download_key == "sm-key"
        fetch.assert_called_once_with("arn:aws:secretsmanager:...:secret:omim")
        # Cached under the plaintext var so error-message redaction can find it too.
        assert os.environ[DOWNLOAD_KEY_ENV_VAR] == "sm-key"


def test_fetch_key_from_secrets_manager_returns_stripped_secret_string():
    with patch("opendatalake.lib.domain.source_configs.omim.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = "  sm-key\n"
        assert _fetch_key_from_secrets_manager("arn:aws:secretsmanager:...:secret:omim") == "sm-key"
        hook_cls.assert_called_once_with(aws_conn_id="aws_default")


def test_fetch_key_from_secrets_manager_decodes_binary_secret():
    with patch("opendatalake.lib.domain.source_configs.omim.SecretsManagerHook") as hook_cls:
        hook_cls.return_value.get_secret.return_value = b"sm-key"
        assert _fetch_key_from_secrets_manager("arn:aws:secretsmanager:...:secret:omim") == "sm-key"


def test_fetch_key_from_secrets_manager_skips_aws_without_an_arn():
    with patch("opendatalake.lib.domain.source_configs.omim.SecretsManagerHook") as hook_cls:
        assert _fetch_key_from_secrets_manager("") == ""
        hook_cls.assert_not_called()


def test_config_missing_key_is_detected():
    assert OmimConfig(download_key="").missing() is True
    assert OmimConfig(download_key="k").missing() is False
