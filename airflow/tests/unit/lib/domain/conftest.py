import pytest

from opendatalake.lib.domain.model.config import DownloadConfig, UpdateMode
from opendatalake.lib.domain.source_configs import (
    ClinvarSourceConfig,
    DBSNPSourceConfig,
    MondoSourceConfig,
    OneThousandGenomesSourceConfig,
)


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
def one_thousand_genomes_source_config() -> OneThousandGenomesSourceConfig:
    return OneThousandGenomesSourceConfig(
        short_name="1000_Genomes",
        display_name="1000 Genomes Project",
        website="https://www.internationalgenome.org/",
        listing_url="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/",
        download_configs=[
            DownloadConfig(
                download_url=lambda version: (
                    f"https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/{version}/"
                    f"ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.{version}.sites.vcf.gz"
                ),
                label="test",
            )
        ],
        update_mode=UpdateMode.AUTO,
    )


@pytest.fixture
def thousand_genomes_listing_html() -> str:
    return """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head><title>Index of /vol1/ftp/release</title></head>
 <body>
<h1>Index of /vol1/ftp/release</h1>
<table>
<tr><td><a href="/vol1/ftp/">Parent Directory</a></td><td>-</td></tr>
<tr><td><a href="2008_12/">2008_12/</a></td><td>2009-02-19 16:41</td></tr>
<tr><td><a href="2009_02/">2009_02/</a></td><td>2009-06-25 11:14</td></tr>
<tr><td><a href="2010_11/">2010_11/</a></td><td>2011-02-16 09:50</td></tr>
<tr><td><a href="20100804/">20100804/</a></td><td>2011-03-28 14:47</td></tr>
<tr><td><a href="20101123/">20101123/</a></td><td>2011-09-28 15:30</td></tr>
<tr><td><a href="20110521/">20110521/</a></td><td>2013-01-08 09:52</td></tr>
<tr><td><a href="20130502/">20130502/</a></td><td>2025-07-04 16:59</td></tr>
</table>
</body></html>
"""


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
