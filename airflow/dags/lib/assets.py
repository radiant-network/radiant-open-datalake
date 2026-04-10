"""Assets definitions for the DAGs."""

from airflow.sdk import Asset, AssetAlias

from dags.lib.config import ASSETS_URI_PREFIX


def new_source_version_asset(source):
    return Asset(
        uri=f"{ASSETS_URI_PREFIX}-new-{source}-version",
        name=source,
    )


check_version_asset_alias = AssetAlias(f"{ASSETS_URI_PREFIX}-check-version-alias")
