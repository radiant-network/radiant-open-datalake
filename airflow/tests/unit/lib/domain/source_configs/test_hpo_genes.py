from unittest.mock import Mock, patch


def test_hpo_genes_get_latest_version(hpo_genes_source_config):
    mock_response = Mock(url="https://github.com/obophenotype/human-phenotype-ontology/releases/tag/v2026-06-23")
    with patch(
        "opendatalake.lib.domain.source_configs.hpo.http_get", return_value=mock_response
    ) as mock_http_get:
        version = hpo_genes_source_config.get_latest_version()

    assert version == "v2026-06-23"
    mock_http_get.assert_called_once_with("https://github.com/obophenotype/human-phenotype-ontology/releases/latest")


def test_hpo_genes_download_url_built_from_version(hpo_genes_source_config):
    url = hpo_genes_source_config.download_configs[0].get_url("v2026-06-23")
    assert url == (
        "https://github.com/obophenotype/human-phenotype-ontology/releases/download/v2026-06-23/genes_to_phenotype.txt"
    )
