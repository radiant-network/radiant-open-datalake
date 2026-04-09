"""Assets definitions for the DAGs."""

from airflow.sdk import Asset, AssetAlias

from dags.lib.config import assets_name_prefix, assets_uri_prefix

new_source_version_asset = Asset(
    uri=f"x-{assets_uri_prefix}-new-source-version",
    name=f"{assets_name_prefix}-new-source-version",
)

check_version_asset_alias = AssetAlias("x-{assets_name_prefix}-check-version-alias")
