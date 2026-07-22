from opendatalake.lib.config import raw_datalake_bucket, raw_landing_prefix, raw_storage_uri


def test_raw_landing_prefix():
    assert raw_landing_prefix("clinvar", "20240101") == "raw/landing/clinvar/20240101"
    assert raw_landing_prefix("dbsnp", "GCF_000001405.40") == "raw/landing/dbsnp/GCF_000001405.40"


def test_raw_storage_uri_matches_landing_prefix_root():
    assert raw_storage_uri() == f"s3a://{raw_datalake_bucket}/raw/landing"
