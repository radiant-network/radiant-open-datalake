from enum import Enum

from dags.lib.domain.model.config import DownloadConfig, UpdateMode
from dags.lib.domain.sources_impl import ClinvarSourceConfig

_VCF_LABEL = "vcf"


# As indicated by the underscore prefix, this enum is intended for internal use within this module only.
# In the future, we may switch to a configuration-based mechanism instead of using an enum.
class _Source(Enum):
    CLINVAR = ClinvarSourceConfig(
        short_name="clinvar",
        display_name="NCBI Clinvar",
        website="https://www.ncbi.nlm.nih.gov/clinvar/",
        download_configs=[
            DownloadConfig(
                download_url="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz",
                md5_present=True,
                label=_VCF_LABEL,
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


###########################################################
# Use the functions below to access source configuration. #
###########################################################


def get_download_configs(source: str) -> list[DownloadConfig]:
    source_enum = _Source[source.upper()]
    return source_enum.value.download_configs


def get_download_config(source: str, index: int) -> DownloadConfig:
    download_configs = get_download_configs(source)
    if index < 0 or index >= len(download_configs):
        raise ValueError(
            f"Download config index {index} invalid for '{source}' (allowed: 0–{len(download_configs) - 1})"
        )
    return download_configs[index]


def get_auto_update_source_ids() -> list[str]:
    return [s.name.lower() for s in _Source if s.value.update_mode == UpdateMode.AUTO]


def get_latest_version(source: str) -> str:
    source_enum = _Source[source.upper()]
    return source_enum.value.get_latest_version()
