from unittest.mock import Mock, patch

import pytest


def test_clinvar_get_latest_version(clinvar_source_config):
    mock_response = Mock()
    mock_response.text = "some text clinvar_20240327.vcf more text"
    with patch("dags.lib.domain.sources_impl.http_get", return_value=mock_response) as mock_http_get:
        version = clinvar_source_config.get_latest_version()
        assert version == "20240327"
        mock_http_get.assert_called_once_with("https://example.com/clinvar.md5")


def test_dbsnp_parse_ref_seq_invalid(dbsnp_source_conf):
    with pytest.raises(ValueError) as excinfo:
        dbsnp_source_conf._parse_ref_seq(filename="invalid_refseq")
    assert excinfo.value.args[0] == "Invalid RefSeq filename: invalid_refseq"


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("GCF_012345678.1", {"prefix": "GCF", "digits": "012345678", "version": 1, "full": "GCF_012345678.1"}),
        ("GCF_012345678.123", {"prefix": "GCF", "digits": "012345678", "version": 123, "full": "GCF_012345678.123"}),
    ],
)
def test_dbsnp_parse_ref_seq_ok(dbsnp_source_conf, filename, expected):
    result = dbsnp_source_conf._parse_ref_seq(filename=filename)
    assert result == expected


def test_dbsnp_get_latest_version(dbsnp_source_conf, dbsnp_valid_listing_html, dbsnp_valid_md5_html):
    listing_response = Mock(text=dbsnp_valid_listing_html)
    md5_response = Mock(text=dbsnp_valid_md5_html)
    with patch(
        "dags.lib.domain.sources_impl.http_get",
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
            "dags.lib.domain.sources_impl.http_get",
            side_effect=[listing_response, md5_response],
        ),
        pytest.raises(ValueError) as excinfo,
    ):
        dbsnp_source_conf.get_latest_version()

    assert (
        excinfo.value.args[0]
        == "Latest RefSeq GCF_000001405.42 is missing .md5 companion at: https://ftp.ncbi.nih.gov/snp/latest_release/VCF/"
    )
