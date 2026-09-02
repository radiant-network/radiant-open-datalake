import pytest

from opendatalake.lib.domain.model.config import DownloadConfig, UpdateMode
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
    SpliceAiSourceConfig,
    TopMedBravoSourceConfig,
)


@pytest.fixture
def gnomad_joint_source_config() -> GnomadJointSourceConfig:
    # Unlike the other sources, every field is `init=False`: the source declares itself entirely.
    return GnomadJointSourceConfig()


@pytest.fixture
def gnomad_cnv_source_config() -> GnomadCnvSourceConfig:
    return GnomadCnvSourceConfig()


@pytest.fixture
def gnomad_sv_source_config() -> GnomadSVSourceConfig:
    return GnomadSVSourceConfig()


@pytest.fixture
def gnomad_constraint_source_config() -> GnomadConstraintSourceConfig:
    return GnomadConstraintSourceConfig()


@pytest.fixture
def spliceai_source_config() -> SpliceAiSourceConfig:
    return SpliceAiSourceConfig()


@pytest.fixture
def omim_source_config() -> OmimSourceConfig:
    return OmimSourceConfig()


@pytest.fixture
def topmed_source_config() -> TopMedBravoSourceConfig:
    return TopMedBravoSourceConfig()


@pytest.fixture
def clinvar_source_config() -> ClinvarSourceConfig:
    return ClinvarSourceConfig(
        short_name="clinvar",
        display_name="ClinVar",
        website="https://www.ncbi.nlm.nih.gov/clinvar/",
        download_configs=[DownloadConfig(download_url="https://example.com/clinvar")],
    )


@pytest.fixture
def dbsnp_source_conf() -> DBSNPSourceConfig:
    return DBSNPSourceConfig(
        short_name="dbsnp",
        display_name="NCBI dbSNP",
        website="https://www.ncbi.nlm.nih.gov/snp/",
        listing_url="https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.{version}.gz"
                ),
                md5_present=True,
                label="test",
            ),
            DownloadConfig(
                download_url=lambda version: (
                    f"https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.{version}.gz.tbi"
                ),
                md5_present=True,
                label="test",
            ),
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def ddd_source_config() -> DDDSourceConfig:
    base_url = "https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads"
    return DDDSourceConfig(
        short_name="ddd",
        display_name="Gene2Phenotype",
        website="https://www.ebi.ac.uk/gene2phenotype/",
        listing_url=f"{base_url}/",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: f"{base_url}/{version}/DDG2P_{version.replace('_', '-')}.csv.gz",
                name="DDG2P.csv.gz",
                md5_present=True,
                label="test",
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def ddd_listing_html() -> str:
    return """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head><title>Index of /pub/databases/gene2phenotype/G2P_data_downloads</title></head>
 <body>
  <table>
   <tr><td><a href="/pub/databases/gene2phenotype/">Parent Directory</a></td></tr>
   <tr><td><a href="2026_05_28/">2026_05_28/</a></td></tr>
   <tr><td><a href="2026_06_28/">2026_06_28/</a></td></tr>
   <tr><td><a href="2026_07_28/">2026_07_28/</a></td></tr>
   <tr><td><a href="Data_download_format_202508-202510.txt">Data_download_format_202508-202510.txt</a></td></tr>
   <tr><td><a href="G2PTermChanges202501.txt">G2PTermChanges202501.txt</a></td></tr>
  </table>
 </body>
</html>
"""


@pytest.fixture
def mondo_source_config() -> MondoSourceConfig:
    return MondoSourceConfig(
        short_name="mondo",
        display_name="Mondo Disease Ontology",
        website="https://mondo.monarchinitiative.org/",
        latest_release_url="https://github.com/monarch-initiative/mondo/releases/latest",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://github.com/monarch-initiative/mondo/releases/download/{version}/mondo-base.obo"
                ),
                label="test",
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def hpo_source_config() -> HpoSourceConfig:
    return HpoSourceConfig(
        short_name="hpo_terms",
        display_name="Human Phenotype Ontology (Terms)",
        website="https://hpo.jax.org/",
        latest_release_url="https://github.com/obophenotype/human-phenotype-ontology/releases/latest",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://github.com/obophenotype/human-phenotype-ontology/releases/download/{version}/hp.obo"
                ),
                label="test",
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def hpo_genes_source_config() -> HpoSourceConfig:
    return HpoSourceConfig(
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
                label="test",
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def dbsnp_valid_listing_html() -> str:
    return """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /snp/latest_release/VCF</title>
 </head>
 <body>
<h1>Index of /snp/latest_release/VCF</h1>
<pre>Name                        Last modified      Size  <hr><a href="/snp/latest_release/">Parent Directory</a>
<a href="CHECKSUMS">CHECKSUMS</a>                   2025-01-15 21:55  224
<a href="GCF_000001405.25.gz">GCF_000001405.25.gz</a>         2025-01-15 19:05   26G
<a href="GCF_000001405.25.gz.md5">GCF_000001405.25.gz.md5</a>     2025-01-15 19:25   54
<a href="GCF_000001405.25.gz.tbi">GCF_000001405.25.gz.tbi</a>     2025-01-15 19:25  2.9M
<a href="GCF_000001405.25.gz.tbi.md5">GCF_000001405.25.gz.tbi.md5</a> 2025-01-15 19:25   58
<a href="GCF_000001405.42.gz">GCF_000001405.42.gz</a>         2025-01-15 21:27   28G
<a href="GCF_000001405.42.gz.md5">GCF_000001405.42.gz.md5</a>     2025-01-15 21:55   54
<a href="GCF_000001405.42.gz.tbi">GCF_000001405.42.gz.tbi</a>     2025-01-15 21:55  3.0M
<a href="GCF_000001405.42.gz.tbi.md5">GCF_000001405.42.gz.tbi.md5</a> 2025-01-15 21:55   58
<hr></pre>
</body></html>
"""  # noqa: E501


@pytest.fixture
def dbsnp_invalid_listing_html_missing_md5(dbsnp_valid_listing_html) -> str:
    _missing = dbsnp_valid_listing_html.replace("GCF_000001405.42.gz.md5", "GCF_000001405.24.gz.md5")
    return _missing


@pytest.fixture
def dbsnp_valid_md5_html() -> str:
    return "6a6f313e92a39c337571174dad12cfe1  GCF_000001405.40.gz"
