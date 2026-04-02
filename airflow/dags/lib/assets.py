"""Assets definitions for the DAGs."""

from airflow.sdk import Asset

from dags.lib.config import assets_name_prefix, assets_uri_prefix, raw_datalake_bucket

new_source_version_asset = Asset(
    uri=f"x-{assets_uri_prefix}-new-source-version",
    name=f"{assets_name_prefix}-new-source-version",
)

raw_dataset_asset = Asset(
    uri=f"s3://{raw_datalake_bucket}/raw",
    name=f"{}{assets_name_prefix}-raw-dataset"
)
