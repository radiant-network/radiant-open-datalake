CHROMOSOMES = [str(c) for c in range(1, 23)] + ["X", "Y"]

CNV_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.1/exome_cnv"
JOINT_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.1/vcf/joint"
SV_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.1/genome_sv"
CONSTRAINT_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/2.1.1/constraint"


def test_gnomad_joint_pins_its_version(gnomad_joint_source_config):
    assert gnomad_joint_source_config.get_latest_version() == "4.1"


def test_gnomad_joint_declares_a_vcf_and_an_index_per_chromosome(gnomad_joint_source_config):
    labels = [c.label for c in gnomad_joint_source_config.download_configs]
    assert labels == [f"{kind}_chr{c}" for c in CHROMOSOMES for kind in ("vcf", "tbi")]


def test_gnomad_joint_urls_match_the_published_layout(gnomad_joint_source_config):
    version = gnomad_joint_source_config.get_latest_version()
    urls = {c.label: c.get_url(version) for c in gnomad_joint_source_config.download_configs}

    # Literal, checked against the gnomAD bucket listing.
    assert urls["vcf_chr1"] == f"{JOINT_RELEASE_ROOT}/gnomad.joint.v4.1.sites.chr1.vcf.bgz"
    assert urls["vcf_chrY"] == f"{JOINT_RELEASE_ROOT}/gnomad.joint.v4.1.sites.chrY.vcf.bgz"

    # Every index sits next to its VCF.
    for chromosome in CHROMOSOMES:
        assert urls[f"tbi_chr{chromosome}"] == urls[f"vcf_chr{chromosome}"] + ".tbi"


def test_gnomad_joint_streams_vcfs_but_not_indexes(gnomad_joint_source_config):
    # VCFs are far too large for a local copy; the tiny indexes go through the ECS path.
    for config in gnomad_joint_source_config.download_configs:
        assert config.use_stream_upload is config.label.startswith("vcf_")


def test_gnomad_joint_declares_no_md5(gnomad_joint_source_config):
    # gnomAD publishes no .md5 alongside the joint VCFs.
    assert all(c.md5_present is False for c in gnomad_joint_source_config.download_configs)


def test_gnomad_cnv_pins_its_version(gnomad_cnv_source_config):
    # The exome CNV files stopped at 4.1; the 4.1.1 patch covers short variants only.
    assert gnomad_cnv_source_config.get_latest_version() == "4.1"


def test_gnomad_cnv_declares_a_single_vcf(gnomad_cnv_source_config):
    config = gnomad_cnv_source_config.download_configs
    assert [c.label for c in config] == ["vcf"]

    # Literal, checked against the gnomAD bucket listing: the full callset, not the non_neuro subsets.
    assert config[0].get_url("4.1") == f"{CNV_RELEASE_ROOT}/gnomad.v4.1.cnv.all.vcf.gz"

    # The file is small enough for the ECS local copy. No .md5 exist next to it in the bucket.
    assert config[0].use_stream_upload is False
    assert config[0].md5_present is False


def test_gnomad_sv_pins_its_version(gnomad_sv_source_config):
    # gnomAD ships SV under 4.0 and 4.1 only, and 4.1.1 has no `genome_sv/` directory.
    assert gnomad_sv_source_config.get_latest_version() == "4.1"


def test_gnomad_sv_urls_match_the_published_layout(gnomad_sv_source_config):
    version = gnomad_sv_source_config.get_latest_version()
    urls = {c.label: c.get_url(version) for c in gnomad_sv_source_config.download_configs}

    # Literal, checked against the gnomAD bucket listing. The full release, not
    # `gnomad.v4.1.sv.non_neuro_controls.sites.vcf.gz`, which restricts the sample set.
    assert urls["vcf"] == f"{SV_RELEASE_ROOT}/gnomad.v4.1.sv.sites.vcf.gz"
    assert urls["tbi"] == urls["vcf"] + ".tbi"
    assert len(urls) == 2


def test_gnomad_sv_streams_the_vcf_but_not_the_index(gnomad_sv_source_config):
    # 1.74 GB of VCF goes straight to S3; the 512 KB index takes the ECS path.
    for config in gnomad_sv_source_config.download_configs:
        assert config.use_stream_upload is (config.label == "vcf")


def test_gnomad_sv_declares_no_md5(gnomad_sv_source_config):
    # gnomAD publishes no .md5 next to the SV files.
    assert all(c.md5_present is False for c in gnomad_sv_source_config.download_configs)


def test_gnomad_constraint_pins_its_version(gnomad_constraint_source_config):
    # Pinned to v2.1.1's flat schema; v4.1.1 is gnomAD's current recommendation but needs a schema
    # migration first (see the comment on GnomadConstraintSourceConfig).
    assert gnomad_constraint_source_config.get_latest_version() == "2.1.1"


def test_gnomad_constraint_declares_a_single_renamed_tsv(gnomad_constraint_source_config):
    config = gnomad_constraint_source_config.download_configs
    assert [c.label for c in config] == ["tsv"]

    # Literal, checked against the gnomAD bucket listing.
    assert config[0].get_url("2.1.1") == f"{CONSTRAINT_RELEASE_ROOT}/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz"

    # bgzip is valid gzip, but Spark's CSV reader only auto-decompresses on a registered .gz suffix.
    assert config[0].name == "gnomad.v2.1.1.lof_metrics.by_gene.txt.gz"

    # The file is small enough for the ECS local copy. No .md5 exists next to it in the bucket.
    assert config[0].use_stream_upload is False
    assert config[0].md5_present is False
