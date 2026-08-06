CHROMOSOMES = [str(c) for c in range(1, 23)] + ["X", "Y"]

RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.1/vcf/joint"
CNV_RELEASE_ROOT = "https://gnomad-public-us-east-1.s3.amazonaws.com/release/4.1/exome_cnv"


def test_gnomad_joint_pins_its_version(gnomad_joint_source_config):
    assert gnomad_joint_source_config.get_latest_version() == "4.1"


def test_gnomad_joint_declares_a_vcf_and_an_index_per_chromosome(gnomad_joint_source_config):
    labels = [c.label for c in gnomad_joint_source_config.download_configs]
    assert labels == [f"{kind}_chr{c}" for c in CHROMOSOMES for kind in ("vcf", "tbi")]


def test_gnomad_joint_urls_match_the_published_layout(gnomad_joint_source_config):
    version = gnomad_joint_source_config.get_latest_version()
    urls = {c.label: c.get_url(version) for c in gnomad_joint_source_config.download_configs}

    # Literal, checked against the gnomAD bucket listing.
    assert urls["vcf_chr1"] == f"{RELEASE_ROOT}/gnomad.joint.v4.1.sites.chr1.vcf.bgz"
    assert urls["vcf_chrY"] == f"{RELEASE_ROOT}/gnomad.joint.v4.1.sites.chrY.vcf.bgz"

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


def test_gnomad_cnv_declares_a_single_streamed_vcf(gnomad_cnv_source_config):
    config = gnomad_cnv_source_config.download_configs
    assert [c.label for c in config] == ["vcf"]

    # Literal, checked against the gnomAD bucket listing: the full callset, not the non_neuro subsets.
    assert config[0].get_url("4.1") == f"{CNV_RELEASE_ROOT}/gnomad.v4.1.cnv.all.vcf.gz"

    # No .tbi and no .md5 exist next to it in the bucket.
    assert config[0].use_stream_upload is True
    assert config[0].md5_present is False
