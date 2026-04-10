from airflow.sdk import dag, task

from dags.lib import config
from dags.lib.assets import new_source_version_asset
from dags.lib.domain.model.sources import get_auto_update_source_ids


def _make_consumer_dag(source_id: str):
    asset = new_source_version_asset(source_id)

    @dag(
        dag_id=f"{config.DAG_ID_PREFIX}-consume-{source_id}",
        dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Consume {source_id.capitalize()}",
        schedule=asset,
        tags=config.DAG_DEFAULT_TAGS,
        catchup=False,
    )
    def _consumer():
        @task(inlets=[asset])
        def consume(triggering_asset_events):
            event = triggering_asset_events[asset][-1]
            print(event)
            # TODO: implement download function like:
            # download_source_data(
            # source=triggering_assert_events[asset][-1].extra['source']),
            # 	    latest_version=triggering_assert_events[asset][-1].extra['latest_version'])
            # 	)

        consume()

    _consumer()


for _source_id in get_auto_update_source_ids():
    _make_consumer_dag(_source_id)
