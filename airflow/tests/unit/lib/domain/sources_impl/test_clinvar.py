from unittest.mock import Mock, patch

from dags.lib.domain.model.config import DownloadConfig
from dags.lib.domain.sources_impl.clinvar import ClinvarSourceConfig


def test_clinvar_get_latest_version():
    source_conf = ClinvarSourceConfig(
        short_name="clinvar",
        display_name="ClinVar",
        website="https://www.ncbi.nlm.nih.gov/clinvar/",
        download_configs=[DownloadConfig(download_url="https://example.com/clinvar")],
    )
    mock_response = Mock()
    mock_response.text = "some text clinvar_20240327.vcf more text"
    with patch("dags.lib.domain.sources_impl.clinvar.http_get", return_value=mock_response) as mock_http_get:
        version = source_conf.get_latest_version()
        assert version == "20240327"
        mock_http_get.assert_called_once_with("https://example.com/clinvar.md5")
