from unittest.mock import Mock, patch

import pytest

_HTTP_GET = "opendatalake.lib.domain.source_configs.one_thousand_genomes.http_get"


def test_get_latest_version_picks_most_recent_release(
    one_thousand_genomes_source_config, thousand_genomes_listing_html
):
    mock_response = Mock(text=thousand_genomes_listing_html)
    with patch(_HTTP_GET, return_value=mock_response) as mock_http_get:
        version = one_thousand_genomes_source_config.get_latest_version()

    assert version == "20130502"
    mock_http_get.assert_called_once_with("https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/")


def test_get_latest_version_orders_by_date_not_lexically(one_thousand_genomes_source_config):
    html = '<a href="2010_11/">2010_11/</a><a href="20101115/">20101115/</a>'
    with patch(_HTTP_GET, return_value=Mock(text=html)):
        assert one_thousand_genomes_source_config.get_latest_version() == "20101115"


def test_get_latest_version_ignores_parent_and_sort_links(one_thousand_genomes_source_config):
    html = '<a href="/vol1/ftp/">Parent Directory</a><a href="?C=N;O=D">Name</a><a href="20130502/">20130502/</a>'
    with patch(_HTTP_GET, return_value=Mock(text=html)):
        assert one_thousand_genomes_source_config.get_latest_version() == "20130502"


def test_get_latest_version_no_release_raises(one_thousand_genomes_source_config):
    with (
        patch(_HTTP_GET, return_value=Mock(text="<html><body>Parent Directory only</body></html>")),
        pytest.raises(ValueError, match="No 1000 Genomes release directory"),
    ):
        one_thousand_genomes_source_config.get_latest_version()


def test_get_latest_version_rejects_int_that_is_not_a_real_date(one_thousand_genomes_source_config):
    html = '<a href="20139999/">20139999/</a>'
    with (
        patch(_HTTP_GET, return_value=Mock(text=html)),
        pytest.raises(ValueError, match="Unrecognized 1000 Genomes release format"),
    ):
        one_thousand_genomes_source_config.get_latest_version()
