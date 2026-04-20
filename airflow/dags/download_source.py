import re
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Metadata, dag, task, task_group

from dags.lib import config
from dags.lib.assets import downloaded_source_asset, new_source_version_asset
from dags.lib.domain.download import S3Downloader
from dags.lib.domain.model.config import DownloadConfig
from dags.lib.domain.model.sources import (
    get_auto_update_source_ids,
    get_download_config_at_index,
    get_download_configs,
)
from dags.lib.operators.ecs import PythonScriptOperator
from dags.lib.tasks import get_version


@task(task_display_name="[PyOp] Direct Upload")
def direct_upload(source: str, prefix: str, version: str, download_index: int):
    """
    Runs a direct upload for a given source and download config.
    Note: We pass only source and download config index (not the DownloadConfig object)
    to avoid serialization  issues and prevent exposing sensitive info in the Airflow UI.
    """
    download_conf = get_download_config_at_index(source, download_index)
    downloader = S3Downloader(s3_prefix=prefix, version=version, download_conf=download_conf)
    downloader.direct_upload()


def upload_via_local_copy(
    task_id: str, source: str, prefix: str, version: str, download_index: int, display_label: str = ""
):
    """
    Creates a PythonScriptOperator to upload files via a local copy.
    Note: We pass only source and download config index (not the DownloadConfig object)
    to avoid serialization issues and prevent exposing sensitive info in the Airflow UI.
    """
    script_args = {
        "source": source,
        "prefix": prefix,
        "version": version,
        "download_index": download_index,
    }
    display_suffix = f" {display_label}" if display_label else ""
    return PythonScriptOperator(
        script_name="/opt/opendatalake/upload_via_local_copy.py",
        script_args=script_args,
        pool=config.DOWNLOAD_TASKS_POOL,
        task_id=task_id,
        task_display_name=f"[ECS] Local Copy Upload{display_suffix}",
    )


def _generate_download_task_id(download_conf: DownloadConfig, rank: int) -> str:
    description = ""
    if download_conf.label:
        description = download_conf.label
    elif download_conf.name:
        description = download_conf.name
    elif download_conf.download_url:
        description = Path(download_conf.download_url).name

    max_length = 50
    sanitized_description = re.sub(r"[^A-Za-z0-9]+", "-", description).strip("-")[:max_length]
    description_part = f"_{sanitized_description}" if sanitized_description else ""

    mode = "direct_upload" if download_conf.use_direct_upload else "local_upload"

    return f"{rank}_{mode}{description_part}"


def _make_download_source_dag(source_id: str):
    input_asset = new_source_version_asset(source_id)
    output_asset = downloaded_source_asset(source_id)

    @dag(
        dag_id=f"{config.DAG_ID_PREFIX}-download-{source_id}",
        dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Download {source_id.capitalize()}",
        schedule=input_asset,
        tags=config.DAG_DEFAULT_TAGS + [f"{config.DAG_ID_PREFIX}_{t}" for t in [source_id, "download"]],
        catchup=False,
    )
    def _download():
        @task(task_display_name="[PyOp] Get S3 Prefix")
        def get_prefix(version):
            return f"raw/{source_id}/{version}"

        @task_group(group_id="download_files")
        def download_files(prefix, version):
            """
            Creates upload tasks for each download config.
            Note: We use a loop (not dynamic mapping) for better UI clarity.
            """
            tasks = []
            for i, download_conf in enumerate(get_download_configs(source_id)):
                task_id = _generate_download_task_id(download_conf, i + 1)
                display_label = (download_conf.label or "").upper()
                if download_conf.use_direct_upload:
                    task = direct_upload.override(
                        task_id=task_id,
                        task_display_name=f"[PyOp] Direct Upload {display_label}".rstrip(),
                    )(source_id, prefix, version, i)
                else:
                    task = upload_via_local_copy(
                        task_id, source_id, prefix, version, i, display_label=display_label
                    )
                tasks.append(task)
            return tasks

        @task(outlets=[output_asset], task_display_name="[PyOp] Finalize Download")
        def finalize_download(version, prefix):
            s3_client = S3Hook(config.s3_conn_id).get_conn()
            s3_client.delete_object(Bucket=config.raw_datalake_bucket, Key=f"{prefix}/.in_progress")
            yield Metadata(asset=output_asset, extra={"version": version})

        version = get_version(input_asset)
        prefix = get_prefix(version)
        download_tasks = download_files(prefix, version)
        finalize_download_task = finalize_download(version, prefix)
        download_tasks >> finalize_download_task

    _download()


for source_id in get_auto_update_source_ids():
    _make_download_source_dag(source_id)
