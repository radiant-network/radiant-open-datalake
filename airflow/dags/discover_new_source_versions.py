from airflow.sdk import Metadata, dag, task, task_group

from dags.lib import config
from dags.lib.assets import new_source_version_asset
from dags.lib.domain.model.sources import get_auto_update_source_ids


@dag(
    dag_display_name=f"{config.dag_display_name_prefix} - Discover New Source Versions",
    dag_id=f"{config.dag_id_prefix}-discover-new-source-versions",
    schedule="0 6 * * *",  # every day at 6 AM
    tags=config.dag_default_tags,
    catchup=False,
)
def discover_new_source_versions():
    @task.virtualenv(requirements=["requests==2.32.5"])
    def check_for_update(source: str):
        import logging

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from dags.lib import config
        from dags.lib.domain.datalake import get_raw_datalake_prefix
        from dags.lib.domain.model.sources import get_latest_version

        latest_version = get_latest_version(source)
        prefix = get_raw_datalake_prefix(source, latest_version)

        s3_hook = S3Hook(config.s3_conn_id)
        bucket = config.raw_datalake_bucket
        if s3_hook.check_for_prefix(bucket_name=bucket, prefix=prefix, delimiter="/"):
            logging.info(
                "Files already present in datalake for version %s of source %s, skipping download",
                latest_version,
                source,
            )
        else:
            logging.info("New version detected for source %s: %s. Will trigger download.", source, latest_version)
            s3_hook.load_string("", key=f"{prefix}/.in_progress", bucket_name=bucket)
            return {"latest_version": latest_version, "source": source}

    @task.short_circuit
    def should_continue(update_info):
        return update_info

    @task(outlets=[new_source_version_asset])
    def emit_asset_event(update_info):
        yield Metadata(new_source_version_asset, update_info)

    @task_group()
    def group(source: str):
        check_for_update_task = check_for_update(source)
        (check_for_update_task >> should_continue(check_for_update_task) >> emit_asset_event(check_for_update_task))

    for source in get_auto_update_source_ids():
        group.override(group_id=f"process_{source}")(source)


discover_new_source_versions()
