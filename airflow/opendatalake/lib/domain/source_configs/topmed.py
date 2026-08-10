"""TOPMed BRAVO (freeze 8, hg38) source configuration.

BRAVO serves the callset as one VCF per chromosome behind a short-lived *signed* link that must be
resolved, per chromosome, from its link API and downloaded with a session cookie. The cookie expires
quickly, so — like the SpliceAI access token — it is stored in Secrets Manager (ARN in
``COOKIE_ARN_ENV_VAR``) and refreshed before a manual run, rather than passed as a DAG param.

The source is ``UpdateMode.MANUAL``: it is never auto-discovered, and the operator supplies the
``version`` label when triggering the download/import DAGs (it becomes the raw ``.../topmed_bravo/<version>/``
partition the Spark job reads).
"""

import os
from dataclasses import dataclass, field
from functools import lru_cache

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode
from opendatalake.lib.utils.http import http_get

# The link API returns {"url": "<signed vcf url>"} for chrom=chr<N>.
_LINK_API_ROOT = "https://api.bravo.sph.umich.edu/ui/link?chrom=chr"
_COOKIE_HEADER = "Cookie"

_AWS_CONN_ID = "aws_default"

COOKIE_ENV_VAR = "OPENDATALAKE_TOPMED_BRAVO_COOKIE"

COOKIE_ARN_ENV_VAR = "OPENDATALAKE_TOPMED_BRAVO_COOKIE_ARN"

# chr1..chr22 + chrX. BRAVO publishes no Y / MT.
_CHROMOSOMES: list[str] = [str(c) for c in range(1, 23)] + ["X"]


def _fetch_cookie_from_secrets_manager(arn: str) -> str:
    if not arn:
        return ""
    secret = SecretsManagerHook(aws_conn_id=_AWS_CONN_ID).get_secret(arn)
    if isinstance(secret, bytes):
        secret = secret.decode("utf-8")
    return secret.strip()


@dataclass(frozen=True)
class TopMedBravoConfig:
    cookie: str

    @classmethod
    @lru_cache(maxsize=1)
    def from_env(cls) -> "TopMedBravoConfig":
        cookie = os.getenv(COOKIE_ENV_VAR, "")
        if not cookie:
            cookie = _fetch_cookie_from_secrets_manager(os.getenv(COOKIE_ARN_ENV_VAR, ""))
        return cls(cookie=cookie)

    def missing(self) -> bool:
        return not self.cookie

    def auth_headers(self) -> dict:
        if self.missing():
            raise AirflowException(
                "TOPMed BRAVO cookie is not configured; set the Secrets Manager ARN "
                f"{COOKIE_ARN_ENV_VAR} (or the cookie value {COOKIE_ENV_VAR})."
            )
        return {_COOKIE_HEADER: self.cookie}


def _auth_headers() -> dict:
    return TopMedBravoConfig.from_env().auth_headers()


def _resolve_download_url(chromosome: str) -> str:
    """Resolve the (short-lived) signed VCF URL for a chromosome from the BRAVO link API."""
    return http_get(f"{_LINK_API_ROOT}{chromosome}", _auth_headers()).json()["url"]


_SECRET_ENV_VARS = ((COOKIE_ENV_VAR, COOKIE_ARN_ENV_VAR),)


def _build_download_configs() -> list[DownloadConfig]:
    # `download_url` is evaluated lazily at download time (get_url(version)); the version is ignored —
    # the URL is resolved per chromosome from the link API. `c=chromosome` freezes the loop variable.
    return [
        DownloadConfig(
            download_url=lambda _version, c=chromosome: _resolve_download_url(c),
            name=f"bravo-dbsnp-chr{chromosome}.vcf.gz",
            headers=_auth_headers,
            use_stream_upload=True,
            md5_present=False,
            label=f"chr{chromosome}",
            secret_env_vars=_SECRET_ENV_VARS,
        )
        for chromosome in _CHROMOSOMES
    ]


@dataclass(frozen=True)
class TopMedBravoSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="topmed_bravo")
    display_name: str = field(init=False, default="TOPMed BRAVO")
    website: str = field(init=False, default="https://legacy.bravo.sph.umich.edu/freeze8/hg38/about")
    download_configs: list[DownloadConfig] = field(init=False, default_factory=_build_download_configs)
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(
        init=False,
        default=ImportConfig(
            spark_command="topmed_bravo",
            spark_conf={"spark.dynamicAllocation.maxExecutors": "16"},
            waiter_max_attempts=960,  # ~16h — whole-genome per-chromosome VCFs are large
        ),
    )
