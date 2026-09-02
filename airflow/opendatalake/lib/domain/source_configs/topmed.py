"""TOPMed BRAVO (freeze 8, hg38) source configuration.

BRAVO serves the callset as one VCF per chromosome behind a short-lived *signed* link that must be
resolved, per chromosome, from its link API and downloaded with a session cookie. The cookie expires
quickly, so it is supplied as the download DAG's ``cookie`` param at trigger time (see
``download_source.py``) rather than through an env var or Secrets Manager.

The source is ``UpdateMode.MANUAL``: it is never auto-discovered, and the operator supplies the
``version`` label when triggering the download/import DAGs (it becomes the raw ``.../topmed_bravo/<version>/``
partition the Spark job reads).
"""

from dataclasses import dataclass, field

from airflow.exceptions import AirflowException

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode
from opendatalake.lib.utils.http import http_get

# The link API returns {"url": "<signed vcf url>"} for chrom=chr<N>.
_LINK_API_ROOT = "https://api.bravo.sph.umich.edu/ui/link?chrom=chr"
_COOKIE_HEADER = "Cookie"

# chr1..chr22 + chrX. BRAVO publishes no Y / MT.
_CHROMOSOMES: list[str] = [str(c) for c in range(1, 23)] + ["X"]

_cookie: str | None = None


def set_cookie(cookie: str) -> None:
    global _cookie
    _cookie = cookie


def _auth_headers() -> dict:
    if not _cookie:
        raise AirflowException(
            "TOPMed BRAVO cookie is not set; trigger the download DAG with the 'cookie' param."
        )
    return {_COOKIE_HEADER: _cookie}


def _resolve_download_url(chromosome: str) -> str:
    """Resolve the (short-lived) signed VCF URL for a chromosome from the BRAVO link API."""
    return http_get(f"{_LINK_API_ROOT}{chromosome}", _auth_headers()).json()["url"]


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
            cookie_from_param=True,
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
