from enum import Enum

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode
from opendatalake.lib.domain.source_configs import (
    ClinvarSourceConfig,
    DBSNPSourceConfig,
    GnomadJointSourceConfig,
    HpoSourceConfig,
    MondoSourceConfig,
)

_VCF_LABEL = "vcf"
_TBI_LABEL = "tbi"
_OBO_LABEL = "obo"
_TSV_LABEL = "tsv"


# As indicated by the underscore prefix, this enum is intended for internal use within this module only.
# In the future, we may switch to a configuration-based mechanism instead of using an enum.
class _Source(Enum):
    OneThousandGenomes = SourceConfig(
        short_name="1000_Genomes",
        display_name="1000 Genomes Project",
        website="https://www.internationalgenome.org/home",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz"
                ),
                md5_present=False,
                label=_VCF_LABEL,
                use_stream_upload=True,
            ),
            DownloadConfig(
                download_url=lambda version: (
                    "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz.tbi"
                ),
                md5_present=False,
                label=_TBI_LABEL,
            ),
        ],
        update_mode=UpdateMode.MANUAL,
        import_config=ImportConfig(spark_command="1000genomes"),
    )
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
        import_config=ImportConfig(spark_command="clinvar"),
    )
    DBSNP = DBSNPSourceConfig(
        short_name="dbsnp",
        display_name="NCBI dbSNP",
        website="https://www.ncbi.nlm.nih.gov/snp/",
        listing_url="https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: f"https://ftp.ncbi.nih.gov/snp/latest_release/VCF/{version}.gz",
                md5_present=True,
                label=_VCF_LABEL,
                use_stream_upload=True,
            ),
            DownloadConfig(
                download_url=lambda version: f"https://ftp.ncbi.nih.gov/snp/latest_release/VCF/{version}.gz.tbi",
                md5_present=True,
                label=_TBI_LABEL,
            ),
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(
            spark_command="dbsnp",
            spark_conf={"spark.dynamicAllocation.maxExecutors": "16"},
            waiter_max_attempts=960,  # ~16h
        ),
    )

    GNOMAD_JOINT = GnomadJointSourceConfig()

    MONDO = MondoSourceConfig(
        short_name="mondo",
        display_name="Mondo Disease Ontology",
        website="https://mondo.monarchinitiative.org/",
        latest_release_url="https://github.com/monarch-initiative/mondo/releases/latest",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://github.com/monarch-initiative/mondo/releases/download/{version}/mondo-base.obo"
                ),
                label=_OBO_LABEL,
            )
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(spark_command="mondo"),
    )

    HPO_TERMS = HpoSourceConfig(
        short_name="hpo_terms",
        display_name="Human Phenotype Ontology (Terms)",
        website="https://hpo.jax.org/",
        latest_release_url="https://github.com/obophenotype/human-phenotype-ontology/releases/latest",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://github.com/obophenotype/human-phenotype-ontology/releases/download/{version}/hp.obo"
                ),
                label=_OBO_LABEL,
            )
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(spark_command="hpo_terms"),
    )

    HPO_GENES = HpoSourceConfig(
        short_name="hpo_genes",
        display_name="Human Phenotype Ontology (Genes)",
        website="https://hpo.jax.org/",
        latest_release_url="https://github.com/obophenotype/human-phenotype-ontology/releases/latest",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    "https://github.com/obophenotype/human-phenotype-ontology/releases/download/"
                    f"{version}/genes_to_phenotype.txt"
                ),
                label=_TSV_LABEL,
            )
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(spark_command="hpo_genes"),
    )


###########################################################
# Use the functions below to access source configuration. #
###########################################################


def _normalize_source_id(source: str) -> str:
    return source.lower()


_SOURCE_BY_ID: dict[str, _Source] = {_normalize_source_id(member.value.short_name): member for member in _Source}
assert len(_SOURCE_BY_ID) == len(_Source), "source ids are not unique after normalization"


def _get_source(source: str) -> _Source:
    return _SOURCE_BY_ID[_normalize_source_id(source)]


def get_download_configs(source: str) -> list[DownloadConfig]:
    return _get_source(source).value.download_configs


def get_download_config_at_index(source: str, index: int) -> DownloadConfig:
    download_configs = get_download_configs(source)
    if not 0 <= index < len(download_configs):
        raise IndexError(
            f"Download config index {index} invalid for '{source}' (allowed: 0–{len(download_configs) - 1})"
        )
    return download_configs[index]


def get_display_name(source: str) -> str:
    return _get_source(source).value.display_name


def get_import_config(source: str) -> ImportConfig:
    source_enum = _get_source(source)
    import_config = source_enum.value.import_config
    if import_config is None:
        raise ValueError(f"Source '{source}' has no import_config; declare one in sources.py to import it.")
    return import_config


def get_auto_update_source_ids() -> list[str]:
    return [_normalize_source_id(s.value.short_name) for s in _Source if s.value.update_mode == UpdateMode.AUTO]


def get_latest_version(source: str) -> str:
    return _get_source(source).value.get_latest_version()
