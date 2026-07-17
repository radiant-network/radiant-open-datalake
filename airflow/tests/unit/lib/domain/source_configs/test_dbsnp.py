from unittest.mock import Mock, patch

import pytest


def test_dbsnp_parse_ref_seq_invalid(dbsnp_source_conf):
    with pytest.raises(ValueError) as excinfo:
        dbsnp_source_conf._parse_ref_seq(filename="invalid_refseq")
    assert excinfo.value.args[0] == "Invalid RefSeq filename: invalid_refseq"


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("GCF_000001405.1", {"accession": "GCF_000001405", "version": 1, "full": "GCF_000001405.1"}),
        ("GCF_000001405.123", {"accession": "GCF_000001405", "version": 123, "full": "GCF_000001405.123"}),
    ],
)
def test_dbsnp_parse_ref_seq_ok(dbsnp_source_conf, filename, expected):
    result = dbsnp_source_conf._parse_ref_seq(filename=filename)
    assert result == expected


def test_dbsnp_get_latest_version(dbsnp_source_conf, dbsnp_valid_listing_html, dbsnp_valid_md5_html):
    listing_response = Mock(text=dbsnp_valid_listing_html)
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with patch(
        "opendatalake.lib.domain.source_configs.dbsnp.http_get",
        side_effect=[listing_response, md5_response],
    ) as mock_http_get:
        version = dbsnp_source_conf.get_latest_version()

    assert version == "GCF_000001405.42"
    assert mock_http_get.call_count == 2
    mock_http_get.assert_any_call("https://ftp.ncbi.nih.gov/snp/latest_release/VCF/")
    mock_http_get.assert_any_call("https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.42.gz.md5")


def test_dbsnp_get_latest_version_missing_md5_error(
    dbsnp_source_conf, dbsnp_invalid_listing_html_missing_md5, dbsnp_valid_md5_html
):
    listing_response = Mock(text=dbsnp_invalid_listing_html_missing_md5)
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with (
        patch(
            "opendatalake.lib.domain.source_configs.dbsnp.http_get",
            side_effect=[listing_response, md5_response],
        ),
        pytest.raises(ValueError) as excinfo,
    ):
        dbsnp_source_conf.get_latest_version()

    assert (
        excinfo.value.args[0]
        == "Latest RefSeq GCF_000001405.42 is missing .md5 companion at: https://ftp.ncbi.nih.gov/snp/latest_release/VCF/"
    )


def test_dbsnp_get_latest_version_no_accessions_raises(dbsnp_source_conf):
    html_without_gz = """<html><body>
    <a href="CHECKSUMS">CHECKSUMS</a>
    <a href="README.txt">README.txt</a>
    </body></html>"""
    with (
        patch("opendatalake.lib.domain.source_configs.dbsnp.http_get", return_value=Mock(text=html_without_gz)),
        pytest.raises(ValueError) as excinfo,
    ):
        dbsnp_source_conf.get_latest_version()

    assert excinfo.value.args[0] == "No RefSeq accessions found at: https://ftp.ncbi.nih.gov/snp/latest_release/VCF/"


def test_dbsnp_get_latest_version_picks_highest_version(dbsnp_source_conf, dbsnp_valid_md5_html):
    listing_html = """<html><body>
    <a href="GCF_000001405.25.gz">GCF_000001405.25.gz</a>
    <a href="GCF_000001405.25.gz.md5">GCF_000001405.25.gz.md5</a>
    <a href="GCF_000001405.40.gz">GCF_000001405.40.gz</a>
    <a href="GCF_000001405.40.gz.md5">GCF_000001405.40.gz.md5</a>
    <a href="GCF_000001405.42.gz">GCF_000001405.42.gz</a>
    <a href="GCF_000001405.42.gz.md5">GCF_000001405.42.gz.md5</a>
    </body></html>"""
    listing_response = Mock(text=listing_html)
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with patch(
        "opendatalake.lib.domain.source_configs.dbsnp.http_get",
        side_effect=[listing_response, md5_response],
    ):
        version = dbsnp_source_conf.get_latest_version()

    assert version == "GCF_000001405.42"


def test_dbsnp_verify_md5_digest_ok(dbsnp_source_conf, dbsnp_valid_md5_html):
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with patch("opendatalake.lib.domain.source_configs.dbsnp.http_get", return_value=md5_response) as mock_http_get:
        dbsnp_source_conf._verify_md5_digest(
            listing_url="https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
            version=42,
        )

    mock_http_get.assert_called_once_with("https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.42.gz.md5")


def test_dbsnp_verify_md5_digest_strips_trailing_slashes(dbsnp_source_conf, dbsnp_valid_md5_html):
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with patch("opendatalake.lib.domain.source_configs.dbsnp.http_get", return_value=md5_response) as mock_http_get:
        dbsnp_source_conf._verify_md5_digest(
            listing_url="https://ftp.ncbi.nih.gov/snp/latest_release/VCF///",
            version=42,
        )

    mock_http_get.assert_called_once_with("https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.42.gz.md5")


def test_dbsnp_verify_md5_digest_invalid_raises(dbsnp_source_conf):
    invalid_body = "not-a-valid-md5-hash"
    md5_response = Mock(text=invalid_body)
    with (
        patch("opendatalake.lib.domain.source_configs.dbsnp.http_get", return_value=md5_response),
        pytest.raises(ValueError) as excinfo,
    ):
        dbsnp_source_conf._verify_md5_digest(
            listing_url="https://ftp.ncbi.nih.gov/snp/latest_release/VCF/",
            version=42,
        )

    assert excinfo.value.args[0] == f"Invalid MD5 digest retrieved from {invalid_body}"
    assert isinstance(excinfo.value.__cause__, AttributeError)
