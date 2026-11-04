from dataclasses import dataclass, field

from opendatalake.lib.domain.model.config import DownloadConfig, ImportConfig, SourceConfig, UpdateMode

_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release"

# gnomAD publishes each dataset on its own cadence, so each source declares its own versions: the releases
# its Spark normalizer is known to parse, oldest first. The DAGs offer exactly these, and every URL below is
# built from the version picked at trigger time, so the file downloaded is the release the branch is named after.
JOINT_VERSIONS = ("4.1",)
CNV_VERSIONS = ("4.1",)  # The exome CNV files stopped at 4.1; the 4.1.1 patch covers short variants only.
SV_VERSIONS = ("4.1",)  # gnomAD ships SV under 4.0 and 4.1 only; 4.1.1 has no `genome_sv/` directory.
CONSTRAINT_VERSIONS = ("4.1.2",)


def _joint_vcf_url(version: str, chromosome: str) -> str:
    return f"{_RELEASE_ROOT}/{version}/vcf/joint/gnomad.joint.v{version}.sites.chr{chromosome}.vcf.bgz"


def _sv_vcf_url(version: str) -> str:
    return f"{_RELEASE_ROOT}/{version}/genome_sv/gnomad.v{version}.sv.sites.vcf.gz"


# One pair of vcf, tbi file per chromosome. `c=chromosome` freezes the loop variable in each lambda.
def _build_joint_download_configs() -> list[DownloadConfig]:
    configs = []
    for chromosome in [str(c) for c in range(1, 23)] + ["X", "Y"]:
        configs.append(
            DownloadConfig(
                download_url=lambda version, c=chromosome: _joint_vcf_url(version, c),
                use_stream_upload=True,
                md5_present=False,
                label=f"vcf_chr{chromosome}",
            )
        )
        configs.append(
            DownloadConfig(
                download_url=lambda version, c=chromosome: f"{_joint_vcf_url(version, c)}.tbi",
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
    supported_versions: tuple[str, ...] | None = field(init=False, default=JOINT_VERSIONS)


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
                download_url=lambda version: f"{_RELEASE_ROOT}/{version}/exome_cnv/gnomad.v{version}.cnv.all.vcf.gz",
                md5_present=False,
                label="vcf",
            )
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_cnv"))
    supported_versions: tuple[str, ...] | None = field(init=False, default=CNV_VERSIONS)


# The gnomAD SV callset is genomes-only. We take the full release, not the non_neuro_controls subset
@dataclass(frozen=True)
class GnomadSVSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_sv")
    display_name: str = field(init=False, default="gnomAD Structural Variants")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/data#v4-structural-variants")
    download_configs: list[DownloadConfig] = field(
        init=False,
        default_factory=lambda: [
            DownloadConfig(download_url=_sv_vcf_url, use_stream_upload=True, md5_present=False, label="vcf"),
            DownloadConfig(download_url=lambda version: f"{_sv_vcf_url(version)}.tbi", md5_present=False, label="tbi"),
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_sv"))
    supported_versions: tuple[str, ...] | None = field(init=False, default=SV_VERSIONS)


# Parsed by the Spark contract MAJOR 1 (normalized/gnomad/GnomadConstraint_v1.scala), which is built against
# the v4 constraint_metrics file: dotted column names such as `lof.pLI` and JSON-array columns. The v2.1.1
# lof_metrics.by_gene file is not supported. 4.1.1 has the same header as 4.1.2 but is not listed, because no
# test runs 4.1.1 data; add a release to CONSTRAINT_VERSIONS only once the normalizer is checked against it.
# bgzip is valid gzip, but Spark's CSV reader only auto-decompresses on a registered codec suffix
# (.gz), so the upload is renamed from .tsv.bgz to .tsv.gz.
@dataclass(frozen=True)
class GnomadConstraintSourceConfig(SourceConfig):
    short_name: str = field(init=False, default="gnomad_constraint")
    display_name: str = field(init=False, default="gnomAD Constraint")
    website: str = field(init=False, default="https://gnomad.broadinstitute.org/")
    download_configs: list[DownloadConfig] = field(
        init=False,
        default_factory=lambda: [
            DownloadConfig(
                download_url=lambda version: (
                    f"{_RELEASE_ROOT}/{version}/constraint/gnomad.v{version}.constraint_metrics.tsv.bgz"
                ),
                name=lambda version: f"gnomad.v{version}.constraint_metrics.tsv.gz",
                md5_present=False,
                label="tsv",
            )
        ],
    )
    update_mode: UpdateMode = field(init=False, default=UpdateMode.MANUAL)
    import_config: ImportConfig | None = field(init=False, default=ImportConfig(spark_command="gnomad_constraint"))
    supported_versions: tuple[str, ...] | None = field(init=False, default=CONSTRAINT_VERSIONS)
