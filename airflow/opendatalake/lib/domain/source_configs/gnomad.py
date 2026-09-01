from dataclasses import dataclass, field
from typing import override

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode

_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release"

# gnomAD publishes each dataset on its own cadence, so we pin the two datasets separately
JOINT_VERSION = "4.1"
CNV_VERSION = "4.1"
SV_VERSION = "4.1"

_SV_VCF_URL = f"{_RELEASE_ROOT}/{SV_VERSION}/genome_sv/gnomad.v{SV_VERSION}.sv.sites.vcf.gz"

CONSTRAINT_VERSION = "2.1.1"
_CONSTRAINT_URL = (
    f"{_RELEASE_ROOT}/{CONSTRAINT_VERSION}/constraint/gnomad.v{CONSTRAINT_VERSION}.lof_metrics.by_gene.txt.bgz"
)


def _joint_vcf_url(chromosome: str) -> str:
    return f"{_RELEASE_ROOT}/{JOINT_VERSION}/vcf/joint/gnomad.joint.v{JOINT_VERSION}.sites.chr{chromosome}.vcf.bgz"


# One pair of vcf, tbi file per chromosome
def _build_joint_download_configs() -> list[DownloadConfig]:
    configs = []
    for chromosome in [str(c) for c in range(1, 23)] + ["X", "Y"]:
        vcf_url = _joint_vcf_url(chromosome)
        configs.append(
            DownloadConfig(
                download_url=vcf_url,
                use_stream_upload=True,
                md5_present=False,
                label=f"vcf_chr{chromosome}",
            )
        )
        configs.append(
            DownloadConfig(
                download_url=f"{vcf_url}.tbi",
                md5_present=False,
                label=f"tbi_chr{chromosome}",
            )
        )
    return configs


# We use the gnomad joint dataset, which combines exomes and genomes data.
@dataclass(frozen=True)
class GnomadJointSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_joint")
    display_name: str = field(init=False, default="gnomAD Joint Frequency")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/")
    download_configs: list[DownloadConfig] = field(init=False, default_factory=_build_joint_download_configs)
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(
        init=False,
        default=ImportConfig(
            spark_command="gnomad_joint",
            spark_conf={
                "spark.dynamicAllocation.maxExecutors": "24",
                "spark.hadoop.io.compression.codecs": "io.projectglow.sql.util.BGZFCodec",
            },
            waiter_max_attempts=960,  # ~16h
        ),
    )

    @override
    def get_latest_version(self) -> str:
        return JOINT_VERSION


# gnomAD publishes three exome CNV VCFs (all, non_neuro, non_neuro_controls); we take "all"
@dataclass(frozen=True)
class GnomadCnvSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_cnv")
    display_name: str = field(init=False, default="gnomAD Exome CNV")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/")
    download_configs: list[DownloadConfig] = field(
        init=False,
        default_factory=lambda: [
            DownloadConfig(
                download_url=f"{_RELEASE_ROOT}/{CNV_VERSION}/exome_cnv/gnomad.v{CNV_VERSION}.cnv.all.vcf.gz",
                md5_present=False,
                label="vcf",
            )
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_cnv"))

    @override
    def get_latest_version(self) -> str:
        return CNV_VERSION


# The gnomAD SV callset is genomes-only. We take the full release, not the non_neuro_controls subset
@dataclass(frozen=True)
class GnomadSVSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_sv")
    display_name: str = field(init=False, default="gnomAD Structural Variants")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/data#v4-structural-variants")
    download_configs: list[DownloadConfig] = field(
        init=False,
        default_factory=lambda: [
            DownloadConfig(download_url=_SV_VCF_URL, use_stream_upload=True, md5_present=False, label="vcf"),
            DownloadConfig(download_url=f"{_SV_VCF_URL}.tbi", md5_present=False, label="tbi"),
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_sv"))

    @override
    def get_latest_version(self) -> str:
        return SV_VERSION


# Pinned to v2.1.1, not the current v4.1.1: gnomAD now recommends v4.1.1's LOEUF threshold over v2's
# (https://gnomad.broadinstitute.org/news/2026-03-gnomad-v4-1-1/), but v4.1.1's constraint file
# (gnomad.v4.1.1.constraint_metrics.tsv.bgz) uses a different, nested/array-valued schema (e.g.
# `lof.pLI`, per-ancestry array columns) than v2.1.1's flat lof_metrics.by_gene columns that
# normalized/gnomad/GnomadConstraint.scala and enriched/Genes.scala are built against. Moving to
# v4.1.1 needs a schema migration there first (tracked as follow-up), not just a version bump here.
# bgzip is valid gzip, but Spark's CSV reader only auto-decompresses on a registered codec suffix
# (.gz), so the upload is renamed from .txt.bgz to .txt.gz.
@dataclass(frozen=True)
class GnomadConstraintSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_constraint")
    display_name: str = field(init=False, default="gnomAD Constraint")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/")
    download_configs: list[DownloadConfig] = field(
        init=False,
        default_factory=lambda: [
            DownloadConfig(
                download_url=_CONSTRAINT_URL,
                name=f"gnomad.v{CONSTRAINT_VERSION}.lof_metrics.by_gene.txt.gz",
                md5_present=False,
                label="tsv",
            )
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_constraint"))

    @override
    def get_latest_version(self) -> str:
        return CONSTRAINT_VERSION
