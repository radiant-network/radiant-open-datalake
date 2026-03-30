import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Metadata, dag, task

from dags.lib import config
from dags.lib.assets import new_source_version_asset
from dags.lib.domain.datalake import get_raw_datalake_prefix
from dags.lib.domain.model.sources import get_auto_update_source_ids, get_latest_version


@dag(
    dag_display_name=f"{config.dag_display_name_prefix} - Discover New Source Versions",
    dag_id=f"{config.dag_id_prefix}-discover-new-source-versions",
    schedule="0 6 * * *",  # every day at 6 AM
    tags=config.dag_default_tags,
    catchup=False,
)
def discover_new_source_versions():

    @task(outlets=[new_source_version_asset])
    def poll(source: str):
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
            yield Metadata(new_source_version_asset, {"latest_version": latest_version})

    for source in get_auto_update_source_ids():
        poll.override(task_id=f"poll-{source}")(source)


discover_new_source_versions()
