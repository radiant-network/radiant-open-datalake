from enum import Enum

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode
from opendatalake.lib.domain.source_configs import (
    ClinvarSourceConfig,
    DBSNPSourceConfig,
    DDDSourceConfig,
    GnomadCnvSourceConfig,
    GnomadConstraintSourceConfig,
    GnomadJointSourceConfig,
    GnomadSVSourceConfig,
    HpoSourceConfig,
    MondoSourceConfig,
    OmimSourceConfig,
    OrphanetSourceConfig,
    SpliceAiSourceConfig,
    TopMedBravoSourceConfig,
)

_VCF_LABEL = "vcf"
_TBI_LABEL = "tbi"
_OBO_LABEL = "obo"
_TSV_LABEL = "tsv"
_CSV_LABEL = "csv"
_XML_LABEL = "xml"
_VARIANT_LABEL = "variant"

_DBNSFP_MEMBER_PATTERN = "*_variant.chr*.gz"


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
            spark_conf={
                "spark.dynamicAllocation.maxExecutors": "16",
                "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
                "spark.emr-serverless.executor.disk": "60G",
            },
            waiter_max_attempts=960,  # ~16h
        ),
    )

    GNOMAD_JOINT = GnomadJointSourceConfig()

    GNOMAD_CNV = GnomadCnvSourceConfig()

    GNOMAD_SV = GnomadSVSourceConfig()

    GNOMAD_CONSTRAINT = GnomadConstraintSourceConfig()

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

    DDD = DDDSourceConfig(
        short_name="ddd",
        display_name="Gene2Phenotype",
        website="https://www.ebi.ac.uk/gene2phenotype/",
        listing_url="https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/"
                    f"{version}/DDG2P_{version.replace('_', '-')}.csv.gz"
                ),
                name="DDG2P.csv.gz",
                md5_present=True,
                label=_CSV_LABEL,
            )
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(spark_command="ddd"),
    )

    SPLICEAI = SpliceAiSourceConfig()

    TOPMED_BRAVO = TopMedBravoSourceConfig()

    DBNSFP = SourceConfig(
        short_name="dbnsfp",
        display_name="dbNSFP",
        website="https://www.dbnsfp.org/",
        download_configs=[
            DownloadConfig(
                url_from_param=True,
                use_stream_unzip=True,
                member_pattern=_DBNSFP_MEMBER_PATTERN,
                label=_VARIANT_LABEL,
            )
        ],
        update_mode=UpdateMode.MANUAL,
        import_config=ImportConfig(
            spark_command="dbnsfp",
            spark_conf={
                "spark.dynamicAllocation.maxExecutors": "16",
                "spark.dynamicAllocation.initialExecutors": "5",
                "spark.executor.cores": "4",
                "spark.executor.memory": "16g",
                "spark.executor.memoryOverhead": "2g",
                "spark.emr-serverless.executor.disk.type": "shuffle_optimized",
                "spark.emr-serverless.executor.disk": "100G",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
            waiter_max_attempts=960,  # ~16h
        ),
    )

    OMIM = OmimSourceConfig()

    ORPHANET = OrphanetSourceConfig(
        short_name="orphanet",
        display_name="Orphanet",
        website="https://www.orphadata.com/",
        download_configs=[
            DownloadConfig(
                download_url="https://www.orphadata.com/data/xml/en_product6.xml",  # gene-disorder associations
                name="en_product6.xml",
                label=_XML_LABEL,
            ),
            DownloadConfig(
                download_url="https://www.orphadata.com/data/xml/en_product9_ages.xml",  # disorder ages/inheritance
                name="en_product9_ages.xml",
                label=_XML_LABEL,
            ),
        ],
        update_mode=UpdateMode.AUTO,
        import_config=ImportConfig(spark_command="orphanet"),
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


def requires_download_url(source: str) -> bool:
    return any(c.url_from_param for c in get_download_configs(source))


def requires_cookie_param(source: str) -> bool:
    return any(c.cookie_from_param for c in get_download_configs(source))


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


def get_all_source_ids() -> list[str]:
    return [_normalize_source_id(s.value.short_name) for s in _Source]


def is_auto_update(source: str) -> bool:
    return _get_source(source).value.update_mode == UpdateMode.AUTO


def get_update_mode(source: str) -> str:
    return _get_source(source).value.update_mode.value


def get_latest_version(source: str) -> str:
    return _get_source(source).value.get_latest_version()
