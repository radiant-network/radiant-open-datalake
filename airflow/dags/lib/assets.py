"""Assets definitions for the DAGs."""

from airflow.sdk import Asset, AssetAlias

from dags.lib.config import ASSETS_NAME_PREFIX, ASSETS_URI_PREFIX

new_source_version_asset = Asset(
    uri=f"x-{ASSETS_URI_PREFIX}-new-source-version",
    name=f"{ASSETS_NAME_PREFIX}-new-source-version",
)

check_version_asset_alias = AssetAlias(f"x-{ASSETS_URI_PREFIX}-check-version-alias")
