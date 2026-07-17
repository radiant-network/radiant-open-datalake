"""Assets definitions for the DAGs."""

from airflow.sdk import Asset, AssetAlias

from opendatalake.lib.config import ASSETS_URI_PREFIX

new_source_version_asset_alias = AssetAlias(f"x-{ASSETS_URI_PREFIX}-new-source-version-alias")


def new_source_version_asset(source):
    return Asset(
        uri=f"{ASSETS_URI_PREFIX}-new-{source}-version",
        name=f"{ASSETS_URI_PREFIX} - new {source} version",
    )


def downloaded_source_asset(source):
    return Asset(
        uri=f"{ASSETS_URI_PREFIX}-downloaded-{source}",
        name=f"{ASSETS_URI_PREFIX} - downloaded {source}",
    )
