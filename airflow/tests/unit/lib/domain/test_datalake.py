from dags.lib.domain.datalake import get_raw_datalake_prefix


def test_get_raw_datalake_prefix():
    prefix = get_raw_datalake_prefix(source="clinvar", version="1.0.0")
    assert prefix == "raw/clinvar/1.0.0"
