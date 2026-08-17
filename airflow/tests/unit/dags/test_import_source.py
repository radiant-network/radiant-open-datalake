import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from opendatalake.dags.import_source import build_import_operator
from opendatalake.lib import config
from opendatalake.lib.domain.model.sources import get_import_config
from opendatalake.lib.operators.emr import DEFAULT_ENTRY_CLASS, EmrServerlessConfig


def _build_operator(source_id: str):
    with DAG(dag_id="test_import", schedule=None):
        version = EmptyOperator(task_id="get_version").output
        return build_import_operator(source_id, version), version


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_clinvar", "opendatalake_import", "opendatalake_auto"}


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert set(dag.task_ids) == {"get_version", "run_spark_import", "finalize_import"}


def test_version_xcomarg_wires_dependency(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert dag.get_task("run_spark_import").upstream_task_ids == {"get_version"}


def test_import_emits_output_asset(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    finalize = dag.get_task("finalize_import")
    assert "run_spark_import" in finalize.upstream_task_ids
    assert {a.uri for a in finalize.outlets} == {"opendatalake-imported-clinvar"}


def test_import_dbsnp_dag_exists(dag_bag):
    assert dag_bag.get_dag(dag_id="opendatalake-import-dbsnp") is not None


def test_import_dbnsfp_dag_exists(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-dbnsfp")
    assert dag is not None
    assert not dag_bag.import_errors
    assert "version" in dag.params
    assert "opendatalake_manual" in dag.tags


def test_dbnsfp_import_uses_dbnsfp_command_and_tuning():
    operator, _ = _build_operator("dbnsfp")
    spark_submit = operator.job_driver["sparkSubmit"]
    args = spark_submit["entryPointArguments"]

    assert args[0] == "dbnsfp"
    assert operator.waiter_max_attempts == 960
    assert "spark.dynamicAllocation.maxExecutors=16" in spark_submit["sparkSubmitParameters"]


def test_manual_source_import_dag_exists(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-1000_genomes")
    assert dag is not None
    assert not dag_bag.import_errors
    assert "version" in dag.params
    assert "opendatalake_manual" in dag.tags


def test_import_dag_has_version_param(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert "version" in dag.params


def test_entry_point_arguments():
    operator, version = _build_operator("clinvar")
    spark_submit = operator.job_driver["sparkSubmit"]
    args = spark_submit["entryPointArguments"]

    version_at = args.index("--version")
    assert args[version_at + 1] is version
    assert args[:version_at] == ["clinvar", "--config", f"config/{config.environment}.conf", "--steps", "default"]
    assert args[version_at + 2 :] == [
        "--raw-storage",
        config.raw_storage_uri(),
        "--database",
        config.iceberg_database,
        "--warehouse",
        config.iceberg_warehouse,
    ]

    assert spark_submit["entryPoint"] == EmrServerlessConfig.from_env().jar_s3_path
    assert f"--class {DEFAULT_ENTRY_CLASS}" in spark_submit["sparkSubmitParameters"]


def test_dbsnp_tuning_applied():
    operator, _ = _build_operator("dbsnp")
    spark_submit = operator.job_driver["sparkSubmit"]

    assert operator.waiter_max_attempts == 960
    assert "spark.dynamicAllocation.maxExecutors=16" in spark_submit["sparkSubmitParameters"]


def test_clinvar_uses_default_tuning():
    operator, _ = _build_operator("clinvar")
    spark_submit = operator.job_driver["sparkSubmit"]

    assert operator.waiter_max_attempts == 480
    assert "spark.dynamicAllocation.maxExecutors=4" in spark_submit["sparkSubmitParameters"]


def test_import_config_sourced_from_source_config():
    import_config = get_import_config("dbsnp")
    assert import_config.spark_command == "dbsnp"
    assert import_config.waiter_max_attempts == 960
    assert import_config.spark_conf == {"spark.dynamicAllocation.maxExecutors": "16"}


def test_unknown_source_raises():
    with pytest.raises(KeyError):
        get_import_config("does-not-exist")


def test_import_orphanet_dag_exists(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-orphanet")
    assert dag is not None
    assert not dag_bag.import_errors
    assert "version" in dag.params
    assert "opendatalake_auto" in dag.tags


def test_orphanet_import_config_uses_orphanet_command():
    assert get_import_config("orphanet").spark_command == "orphanet"
