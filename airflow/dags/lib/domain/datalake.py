def get_raw_datalake_prefix(source: str, version: str) -> str:
    """
    Get the S3 prefix where raw files for a given source and version should be stored.

    The prefix is defined as "raw/{source}/{version}".
    It does not include the bucket name.
    """
    return f"raw/{source}/{version}"
