from airflow.sdk import Metadata, XComArg, dag, task

from opendatalake.lib import config
from opendatalake.lib.assets import downloaded_source_asset, imported_source_asset
from opendatalake.lib.domain.model.sources import (
    get_all_source_ids,
    get_display_name,
    get_import_config,
    get_update_mode,
)
from opendatalake.lib.operators.spark_k8s import EmrServerlessJobOperator
from opendatalake.lib.tasks import get_version, version_param


def build_import_operator(source_id: str, version: XComArg) -> EmrServerlessJobOperator:
    import_config = get_import_config(source_id)
    display_name = get_display_name(source_id)

    tuning = {
        k: v
        for k, v in (
            ("spark_conf", import_config.spark_conf),
            ("waiter_max_attempts", import_config.waiter_max_attempts),
        )
        if v is not None
    }

    return EmrServerlessJobOperator(
        task_id="run_spark_import",
        task_display_name=f"[EMR] Import {display_name}",
        name=f"opendatalake-{config.environment}-import-{source_id}-{{{{ ts_nodash }}}}",
        entry_point_arguments=[
            import_config.spark_command,
            "--config",
            f"config/{config.environment}.conf",
            "--steps",
            "default",
            "--version",
            version,
            "--raw-storage",
            config.raw_storage_uri(),
        ],
        **tuning,
    )


def _make_import_source_dag(source_id: str):
    input_asset = downloaded_source_asset(source_id)
    output_asset = imported_source_asset(source_id)
    display_name = get_display_name(source_id)

    @dag(
        dag_id=f"{config.DAG_ID_PREFIX}-import-{source_id}",
        dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Import {display_name}",
        schedule=input_asset,
        params=version_param(),
        tags=config.DAG_DEFAULT_TAGS
        + [f"{config.DAG_ID_PREFIX}_{t}" for t in [source_id, "import", get_update_mode(source_id)]],
        catchup=False,
    )
    def _import():
        @task(outlets=[output_asset], task_display_name="[PyOp] Finalize Import")
        def finalize_import(version):
            yield Metadata(asset=output_asset, extra={"version": version})

        version = get_version(input_asset)
        import_task = build_import_operator(source_id, version)
        import_task >> finalize_import(version)

    _import()


for source_id in get_all_source_ids():
    _make_import_source_dag(source_id)
