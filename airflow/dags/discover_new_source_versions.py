import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Metadata, dag, task

from dags.lib import config
from dags.lib.assets import check_version_asset_alias, new_source_version_asset
from dags.lib.domain.model.sources import get_auto_update_source_ids, get_latest_version


@dag(
    dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Discover New Source Versions",
    dag_id=f"{config.DAG_ID_PREFIX}-discover-new-source-versions",
    schedule="0 6 * * *",  # every day at 6 AM
    tags=config.DAG_DEFAULT_TAGS,
    catchup=False,
)
def discover_new_source_versions():
    # Not using virtualenv operator due to an Airflow 3.0.6 bug blocking access to variables/connections.
    # You might want to revisit if upgrading Airflow.
    @task(outlets=[check_version_asset_alias])
    def check_for_update(source: str):
        latest_version = get_latest_version(source)
        prefix = f"raw/{source}/{latest_version}"

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
            yield Metadata(
                asset=new_source_version_asset,
                extra={"source": source, "latest_version": latest_version},  # extra has to be provided, can be {}
                alias=check_version_asset_alias,
            )

    for source in get_auto_update_source_ids():
        check_for_update.override(task_id=f"{source}_check_for_update")(source=source)


discover_new_source_versions()
