import re
from datetime import timedelta
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Metadata, dag, task, task_group

from opendatalake.lib import config
from opendatalake.lib.assets import downloaded_source_asset, new_source_version_asset
from opendatalake.lib.domain.download import S3Downloader
from opendatalake.lib.domain.model.config import DownloadConfig
from opendatalake.lib.domain.model.sources import (
    get_all_source_ids,
    get_display_name,
    get_download_config_at_index,
    get_download_configs,
    get_update_mode,
    is_auto_update,
    requires_download_url,
)
from opendatalake.lib.operators.ecs import PythonScriptOperator
from opendatalake.lib.tasks import download_url_param, get_version, version_param


@task(pool=config.DIRECT_UPLOAD_TASKS_POOL)
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
    task_id: str,
    source: str,
    prefix: str,
    version: str,
    download_index: int,
    label: str,
    secret_env_vars: tuple[tuple[str, str], ...] = (),
):
    """
    Creates a PythonScriptOperator to upload files via a local copy.
    Note: We pass only source and download config index (not the DownloadConfig object)
    to avoid serialization issues and prevent exposing sensitive info in the Airflow UI.
    `secret_env_vars` carries only names (the container env var and the env var holding its Secrets
    Manager ARN); the operator injects them into the ECS task via `secrets`/`valueFrom`, so the secret
    itself never travels in the RunTask call.
    """
    script_args = {
        "source": source,
        "prefix": prefix,
        "version": version,
        "download_index": download_index,
    }
    return PythonScriptOperator(
        script_name="/opt/opendatalake/upload_via_local_copy.py",
        script_args=script_args,
        pool=config.DOWNLOAD_TASKS_POOL,
        task_id=task_id,
        task_display_name=f"[ECS] Local Copy Upload {label}/{download_index}",
        secret_env_vars=secret_env_vars,
    )


def stream_unzip_download(
    task_id: str,
    source: str,
    prefix: str,
    version: str,
    download_index: int,
    label: str,
):
    script_args = {
        "source": source,
        "prefix": prefix,
        "version": version,
        "download_index": download_index,
        "download_url": "{{ params.download_url }}",
    }
    return PythonScriptOperator(
        script_name="/opt/opendatalake/stream_unzip_download.py",
        script_args=script_args,
        pool=config.DOWNLOAD_TASKS_POOL,
        task_id=task_id,
        task_display_name=f"[ECS] Stream Unzip {label}/{download_index}",
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

    if download_conf.use_stream_unzip:
        mode = "stream_unzip"
    elif download_conf.use_stream_upload:
        mode = "direct_upload"
    else:
        mode = "local_upload"

    return f"{rank}_{mode}{description_part}"


def _make_download_source_dag(source_id: str):
    input_asset = new_source_version_asset(source_id)
    output_asset = downloaded_source_asset(source_id)
    display_name = get_display_name(source_id)

    schedule = input_asset if is_auto_update(source_id) else None

    # Manual URL-based sources (e.g. dbNSFP) take the archive URL at trigger time.
    params = version_param()
    if requires_download_url(source_id):
        params = {**params, **download_url_param()}

    @dag(
        dag_id=f"{config.DAG_ID_PREFIX}-download-{source_id}",
        dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Download {display_name}",
        schedule=schedule,
        params=params,
        tags=config.DAG_DEFAULT_TAGS
        + [f"{config.DAG_ID_PREFIX}_{t}" for t in [source_id, "download", get_update_mode(source_id)]],
        catchup=False,
        # Retries let direct uploads (multipart) resume automatically: a new attempt only
        # downloads the parts that were not already uploaded to S3.
        default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    )
    def _download():
        @task(task_display_name="[PyOp] Get S3 Prefix")
        def get_prefix(version):
            return config.raw_landing_prefix(source_id, version)

        @task_group(group_id="download_files")
        def download_files(prefix, version):
            """
            Creates upload tasks for each download config.
            Note: We use a loop (not dynamic mapping) for better UI clarity.
            """
            tasks = []
            for i, download_conf in enumerate(get_download_configs(source_id)):
                task_id = _generate_download_task_id(download_conf, i + 1)
                if download_conf.use_stream_unzip:
                    task = stream_unzip_download(
                        task_id,
                        source_id,
                        prefix,
                        version,
                        i,
                        download_conf.label or "",
                    )
                elif download_conf.use_stream_upload:
                    task = direct_upload.override(
                        task_id=task_id,
                        task_display_name=f"[PyOp] Direct Upload {download_conf.label}/{i}",
                    )(source_id, prefix, version, i)
                else:
                    task = upload_via_local_copy(
                        task_id,
                        source_id,
                        prefix,
                        version,
                        i,
                        download_conf.label or "",
                        download_conf.secret_env_vars,
                    )
                tasks.append(task)
            return tasks

        @task(outlets=[output_asset], task_display_name="[PyOp] Finalize Download")
        def finalize_download(version, prefix):
            s3_client = S3Hook(config.s3_conn_id).get_conn()
            s3_client.delete_object(Bucket=config.raw_datalake_bucket, Key=f"{prefix}/.in_progress")
            yield Metadata(asset=output_asset, extra={"version": version})

        version = get_version(input_asset, asset_active=is_auto_update(source_id))
        prefix = get_prefix(version)
        download_tasks = download_files(prefix, version)
        finalize_download_task = finalize_download(version, prefix)
        download_tasks >> finalize_download_task

    _download()


for source_id in get_all_source_ids():
    _make_download_source_dag(source_id)
