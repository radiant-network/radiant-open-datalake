import logging

from airflow.sdk import Asset, task

GET_VERSION_TASK_ID = "get_version"


def get_version(asset: Asset, **kwargs):
    @task(task_id=GET_VERSION_TASK_ID, task_display_name="[PyOp] Get Version", inlets=[asset])
    def _get_version(triggering_asset_events):
        events = triggering_asset_events[asset]
        if not events:
            raise ValueError(f"No triggering events found for asset {asset}")
        if len(events) > 1:
            logging.warning(
                f"Multiple triggering events found for asset {asset}, using the latest one. Events: {events}"
            )
        return events[-1].extra["version"]

    return _get_version(**kwargs)
