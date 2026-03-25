from dataclasses import dataclass
from enum import Enum

from dags.lib.domain.model.download_config import DownloadConfig


@dataclass(frozen=True)
class SourceConfig:
    short_name: str
    display_name: str
    website: str
    download_configs: list[DownloadConfig]


class _Source(Enum):
    CLINVAR = SourceConfig(
        short_name="clinvar",
        display_name="NCBI Clinvar",
        website="https://www.ncbi.nlm.nih.gov/clinvar/",
        download_configs=[
            DownloadConfig(url="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz", md5_present=True)
        ],
    )


########################################################################################
# Always use the functions below to access source configuration.                       #
# Avoid direct access to the enum, as we may switch to a configuration-based mechanism #
# in the future. If the existing functions do not fit your needs, add a new one rather #
# than accessing the enum directly.                                                    #
########################################################################################


def get_download_configs(source: str) -> list[DownloadConfig]:
    source_enum = _Source[source.upper()]
    return source_enum.value.download_configs
