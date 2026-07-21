from airflow.exceptions import AirflowException
from airflow.sdk import XComArg, dag

from opendatalake.lib import config
from opendatalake.lib.assets import downloaded_source_asset
from opendatalake.lib.domain.model.sources import get_auto_update_source_ids
from opendatalake.lib.operators.emr import EmrServerlessJobOperator
from opendatalake.lib.tasks import get_version

_SPARK_COMMAND = {
    "clinvar": "clinvar",
    "dbsnp": "dbsnp",
}

_SOURCES_OVERRIDES = {
    "dbsnp": {
        "spark_conf": {"spark.dynamicAllocation.maxExecutors": "16"},
        "waiter_max_attempts": 960,  # ~16h
    },
}


def _spark_command_for(source_id: str) -> str:
    try:
        return _SPARK_COMMAND[source_id]
    except KeyError:
        raise AirflowException(
            f"No Spark command mapped for source '{source_id}'. Add it to _SPARK_COMMAND in import_source.py."
        ) from None


def build_import_operator(source_id: str, version: XComArg) -> EmrServerlessJobOperator:
    command = _spark_command_for(source_id)
    tuning = _SOURCES_OVERRIDES.get(source_id, {})
    return EmrServerlessJobOperator(
        task_id="run_spark_import",
        task_display_name=f"[EMR] Import {source_id.capitalize()}",
        name=f"opendatalake-{config.environment}-import-{source_id}-{{{{ ts_nodash }}}}",
        entry_point_arguments=[
            command,
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

    @dag(
        dag_id=f"{config.DAG_ID_PREFIX}-import-{source_id}",
        dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Import {source_id.capitalize()}",
        schedule=input_asset,
        tags=config.DAG_DEFAULT_TAGS + [f"{config.DAG_ID_PREFIX}_{t}" for t in [source_id, "import"]],
        catchup=False,
    )
    def _import():
        version = get_version(input_asset)
        build_import_operator(source_id, version)

    _import()


for source_id in get_auto_update_source_ids():
    _make_import_source_dag(source_id)
