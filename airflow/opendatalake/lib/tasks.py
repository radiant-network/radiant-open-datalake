import logging

from airflow.sdk import Asset, task
from airflow.sdk.definitions.param import Param


def version_param() -> dict:
    # To ensure one different version param per DAG
    return {
        "version": Param(
            None,
            type=["null", "string"],
            title="Source version",
            description=(
                "Version to process. Required only when triggering this DAG manually; "
                "asset-triggered runs read the version from the upstream event."
            ),
        )
    }


def download_url_param() -> dict:
    return {
        "download_url": Param(
            None,
            type=["null", "string"],
            title="Download URL",
            description=(
                "Direct URL of the archive to download. Required when triggering a manual "
                "URL-based source (e.g. dbNSFP), whose URL is not derivable from the version."
            ),
        )
    }


def _version_from_param(params) -> str | None:
    """The `version` DAG param, or None when it was not supplied."""
    return (params or {}).get("version")


def _resolve_version_from_events(asset: Asset, triggering_asset_events, params) -> str:
    """Version from the latest triggering asset event; fall back to the `version` param (manual trigger)."""
    events = triggering_asset_events[asset]
    if events:
        if len(events) > 1:
            logging.warning(
                f"Multiple triggering events found for asset {asset}, using the latest one. Events: {events}"
            )
        return events[-1].extra["version"]
    version = _version_from_param(params)
    if version:
        return version
    raise ValueError(
        f"No triggering events found for asset {asset} and no 'version' param supplied; "
        "provide a version when triggering this DAG manually."
    )


def _resolve_version_from_param(params) -> str:
    """Version from the `version` param only (inactive/MANUAL input asset -- the task takes no inlet)."""
    version = _version_from_param(params)
    if version:
        return version
    raise ValueError("No 'version' param supplied; provide a version when triggering this DAG manually.")


def get_version(asset: Asset, asset_active: bool = True, **kwargs):
    if asset_active:

        @task(task_id="get_version", task_display_name="[PyOp] Get Version", inlets=[asset])
        def _get_version(triggering_asset_events, params=None):
            return _resolve_version_from_events(asset, triggering_asset_events, params)

    else:

        @task(task_id="get_version", task_display_name="[PyOp] Get Version")
        def _get_version(params=None):
            return _resolve_version_from_param(params)

    return _get_version(**kwargs)
