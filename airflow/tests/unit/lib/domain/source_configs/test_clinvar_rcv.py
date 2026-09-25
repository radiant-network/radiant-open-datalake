from unittest.mock import Mock, patch

import pytest

_LISTING_URL = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/RCV_release/"
_PATCH_TARGET = "opendatalake.lib.domain.source_configs.clinvar.http_get"


def test_clinvar_rcv_get_latest_version(clinvar_rcv_source_config, clinvar_rcv_listing_html):
    with patch(_PATCH_TARGET, return_value=Mock(text=clinvar_rcv_listing_html)) as mock_http_get:
        version = clinvar_rcv_source_config.get_latest_version()

    assert version == "2026-09"
    mock_http_get.assert_called_once_with(_LISTING_URL)


def test_clinvar_rcv_ignores_the_latest_symlink(clinvar_rcv_source_config):
    # `00-latest` is a symlink to the newest monthly release, not a version of its own.
    html = """<a href="ClinVarRCVRelease_00-latest.xml.gz">latest</a>
    <a href="ClinVarRCVRelease_00-latest.xml.gz.md5">latest md5</a>
    <a href="ClinVarRCVRelease_2026-03.xml.gz">march</a>
    <a href="ClinVarRCVRelease_2026-03.xml.gz.md5">march md5</a>"""
    with patch(_PATCH_TARGET, return_value=Mock(text=html)):
        assert clinvar_rcv_source_config.get_latest_version() == "2026-03"


def test_clinvar_rcv_skips_a_release_whose_md5_is_not_published_yet(clinvar_rcv_source_config):
    # NCBI uploads the release and its checksum separately; an incomplete month is not a version yet.
    html = """<a href="ClinVarRCVRelease_2026-08.xml.gz">august</a>
    <a href="ClinVarRCVRelease_2026-08.xml.gz.md5">august md5</a>
    <a href="ClinVarRCVRelease_2026-09.xml.gz">september</a>"""
    with patch(_PATCH_TARGET, return_value=Mock(text=html)):
        assert clinvar_rcv_source_config.get_latest_version() == "2026-08"


def test_clinvar_rcv_no_release_raises(clinvar_rcv_source_config):
    html = """<a href="archive/">archive/</a><a href="README">README</a>"""
    with (
        patch(_PATCH_TARGET, return_value=Mock(text=html)),
        pytest.raises(ValueError) as excinfo,
    ):
        clinvar_rcv_source_config.get_latest_version()

    assert excinfo.value.args[0] == f"No ClinVar RCV releases found at {_LISTING_URL}"


def test_clinvar_rcv_no_checksum_at_all_raises(clinvar_rcv_source_config):
    html = """<a href="ClinVarRCVRelease_2026-09.xml.gz">september</a>"""
    with (
        patch(_PATCH_TARGET, return_value=Mock(text=html)),
        pytest.raises(ValueError) as excinfo,
    ):
        clinvar_rcv_source_config.get_latest_version()

    assert excinfo.value.args[0] == f"No ClinVar RCV release has a .md5 companion at {_LISTING_URL}"


def test_clinvar_rcv_download_url_built_from_version(clinvar_rcv_source_config):
    download_config = clinvar_rcv_source_config.download_configs[0]

    assert download_config.get_url("2026-09") == f"{_LISTING_URL}ClinVarRCVRelease_2026-09.xml.gz"
    assert download_config.md5_present is True
    assert download_config.use_stream_upload is True
