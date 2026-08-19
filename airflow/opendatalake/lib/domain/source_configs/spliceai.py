import os
import re
from dataclasses import dataclass, field
from typing import override

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode
from opendatalake.lib.utils.http import http_get

_API_ROOT = "https://api.basespace.illumina.com/v2/files"
_AUTH_HEADER = "x-access-token"

_AWS_CONN_ID = "aws_default"

ACCESS_TOKEN_ENV_VAR = "OPENDATALAKE_SPLICEAI_ACCESS_TOKEN"

ACCESS_TOKEN_ARN_ENV_VAR = "OPENDATALAKE_SPLICEAI_ACCESS_TOKEN_ARN"


def _fetch_token_from_secrets_manager(arn: str) -> str:
    if not arn:
        return ""
    secret = SecretsManagerHook(aws_conn_id=_AWS_CONN_ID).get_secret(arn)
    if isinstance(secret, bytes):
        secret = secret.decode("utf-8")
    return secret.strip()


# Fixed public score files (not re-released since 2019); versioning tracks their ETags.
_FILE_IDS = {
    "snv": {"vcf": "16525380715", "tbi": "16525505189"},
    "indel": {"vcf": "16525003580", "tbi": "16525276839"},
}

_SAFE_ETAG = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class SpliceAiConfig:
    access_token: str

    @classmethod
    def from_env(cls) -> "SpliceAiConfig":
        token = os.getenv(ACCESS_TOKEN_ENV_VAR, "")
        if not token:
            token = _fetch_token_from_secrets_manager(os.getenv(ACCESS_TOKEN_ARN_ENV_VAR, ""))
        return cls(access_token=token)

    def missing(self) -> bool:
        return not self.access_token

    def auth_headers(self) -> dict:
        if self.missing():
            raise AirflowException(
                "SpliceAI access token is not configured; set the Secrets Manager ARN "
                f"{ACCESS_TOKEN_ARN_ENV_VAR} (or the token value {ACCESS_TOKEN_ENV_VAR})."
            )
        return {_AUTH_HEADER: self.access_token}


def _sanitize_etag(raw: str) -> str:
    etag = raw.strip()
    if etag.startswith("W/"):
        etag = etag[2:]
    etag = etag.strip('"').strip()
    if not _SAFE_ETAG.fullmatch(etag):
        raise AirflowException(f"SpliceAI ETag {raw!r} is not usable as a version (path/branch) segment.")
    return etag


def _content_url(file_id: str) -> str:
    return f"{_API_ROOT}/{file_id}/content"


def _auth_headers() -> dict:
    return SpliceAiConfig.from_env().auth_headers()


_SECRET_ARN_ENV_VARS = (ACCESS_TOKEN_ARN_ENV_VAR,)


def _build_download_configs() -> list[DownloadConfig]:
    configs: list[DownloadConfig] = []
    for variant_type, ids in _FILE_IDS.items():
        file_name = f"spliceai_scores.raw.{variant_type}.hg38.vcf.gz"
        configs.append(
            DownloadConfig(
                download_url=_content_url(ids["vcf"]),
                name=file_name,
                headers=_auth_headers,
                use_stream_upload=True,
                md5_present=False,
                label=f"{variant_type}_vcf",
                secret_arn_env_vars=_SECRET_ARN_ENV_VARS,
            )
        )
        configs.append(
            DownloadConfig(
                download_url=_content_url(ids["tbi"]),
                name=f"{file_name}.tbi",
                headers=_auth_headers,
                md5_present=False,
                label=f"{variant_type}_tbi",
                secret_arn_env_vars=_SECRET_ARN_ENV_VARS,
            )
        )
    return configs


@dataclass(frozen=True)
class SpliceAiSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="spliceai")
    display_name: str = field(init=False, default="SpliceAI")
    website: str = field(init=False, default="https://github.com/Illumina/SpliceAI")
    download_configs: list[DownloadConfig] = field(init=False, default_factory=_build_download_configs)
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(
        init=False,
        default=ImportConfig(
            spark_command="spliceai",
            spark_conf={
                "spark.dynamicAllocation.maxExecutors": "5",
                "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
                "spark.emr-serverless.executor.disk": "190G",
            },
            waiter_max_attempts=960,  # ~16h
        ),
    )

    @override
    def get_latest_version(self) -> str:
        headers = _auth_headers()
        etags = [
            _sanitize_etag(http_get(f"{_API_ROOT}/{_FILE_IDS[variant_type]['vcf']}", headers).json()["ETag"])
            for variant_type in ("snv", "indel")
        ]
        return "_".join(etags)
