import logging

from airflow.sdk import Asset, task
from airflow.sdk.definitions.param import Param

VERSION_PARAM = {
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


def get_version(asset: Asset, **kwargs):
    @task(task_id="get_version", task_display_name="[PyOp] Get Version", inlets=[asset])
    def _get_version(triggering_asset_events, params=None):
        events = triggering_asset_events[asset]
        if events:
            if len(events) > 1:
                logging.warning(
                    f"Multiple triggering events found for asset {asset}, using the latest one. Events: {events}"
                )
            return events[-1].extra["version"]
        # No asset event: manual trigger. Fall back to the `version` DAG param.
        version = (params or {}).get("version")
        if version:
            return version
        raise ValueError(
            f"No triggering events found for asset {asset} and no 'version' param supplied; "
            "provide a version when triggering this DAG manually."
        )

    return _get_version(**kwargs)
