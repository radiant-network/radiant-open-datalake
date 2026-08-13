import os
from dataclasses import dataclass, field

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode

_AWS_CONN_ID = "aws_default"

DOWNLOAD_KEY_ENV_VAR = "OPENDATALAKE_OMIM_DOWNLOAD_KEY"
DOWNLOAD_KEY_ARN_ENV_VAR = "OPENDATALAKE_OMIM_DOWNLOAD_KEY_ARN"

_SECRET_ENV_VARS = ((DOWNLOAD_KEY_ENV_VAR, DOWNLOAD_KEY_ARN_ENV_VAR),)

_DOWNLOADS_ROOT = "https://data.omim.org/downloads"
_FILE_NAME = "genemap2.txt"
_TSV_LABEL = "tsv"


def _fetch_key_from_secrets_manager(arn: str) -> str:
    if not arn:
        return ""
    secret = SecretsManagerHook(aws_conn_id=_AWS_CONN_ID).get_secret(arn)
    if isinstance(secret, bytes):
        secret = secret.decode("utf-8")
    return secret.strip()


@dataclass(frozen=True)
class OmimConfig:
    download_key: str

    @classmethod
    def from_env(cls) -> "OmimConfig":
        key = os.getenv(DOWNLOAD_KEY_ENV_VAR, "")
        if not key:
            key = _fetch_key_from_secrets_manager(os.getenv(DOWNLOAD_KEY_ARN_ENV_VAR, ""))
        return cls(download_key=key)

    def missing(self) -> bool:
        return not self.download_key


def _download_url(_version: str) -> str:
    """OMIM embeds the per-account download key in the URL path. Resolved lazily at download time from
    the env (the operator injects it), so the key never appears in the DAG config, args, or logs."""
    config = OmimConfig.from_env()
    if config.missing():
        raise AirflowException(
            "OMIM download key is not configured; set the Secrets Manager ARN "
            f"{DOWNLOAD_KEY_ARN_ENV_VAR} (or the key value {DOWNLOAD_KEY_ENV_VAR})."
        )
    return f"{_DOWNLOADS_ROOT}/{config.download_key}/{_FILE_NAME}"


def _build_download_configs() -> list[DownloadConfig]:
    return [
        DownloadConfig(
            download_url=_download_url,
            name=_FILE_NAME,
            label=_TSV_LABEL,
            secret_env_vars=_SECRET_ENV_VARS,
        )
    ]


@dataclass(frozen=True)
class OmimSourceConfig(SourceConfig):
    # OMIM has no public listing, so there is no version to discover: MANUAL, and the release label is a
    # user-supplied `version` param at trigger time.
    short_name: str = field(init=False, default="omim")
    display_name: str = field(init=False, default="OMIM")
    website: str = field(init=False, default="https://www.omim.org/")
    download_configs: list[DownloadConfig] = field(init=False, default_factory=_build_download_configs)
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="omim"))
