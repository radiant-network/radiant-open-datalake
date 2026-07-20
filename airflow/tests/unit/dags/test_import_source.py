import pytest
from airflow.exceptions import AirflowException

from opendatalake.dags.import_source import _spark_command_for, build_import_operator
from opendatalake.lib.operators.emr import DEFAULT_ENTRY_CLASS, EmrServerlessConfig


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_clinvar", "opendatalake_import"}


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert dag.task_ids == ["get_version", "run_spark_import"]


def test_import_dbsnp_dag_exists(dag_bag):
    assert dag_bag.get_dag(dag_id="opendatalake-import-dbsnp") is not None


def test_entry_point_arguments():
    operator = build_import_operator("clinvar")
    spark_submit = operator.job_driver["sparkSubmit"]

    assert spark_submit["entryPointArguments"] == [
        "clinvar",
        "--config",
        "config/dev.conf",
        "--steps",
        "default",
        "--version",
        "{{ ti.xcom_pull(task_ids='get_version') }}",
        "--raw-storage",
        "s3a://opendatalake-dev/raw/landing",
    ]
    assert spark_submit["entryPoint"] == EmrServerlessConfig.from_env().jar_s3_path
    assert f"--class {DEFAULT_ENTRY_CLASS}" in spark_submit["sparkSubmitParameters"]


def test_dbsnp_tuning_applied():
    operator = build_import_operator("dbsnp")
    spark_submit = operator.job_driver["sparkSubmit"]

    assert operator.waiter_max_attempts == 960
    assert "spark.dynamicAllocation.maxExecutors=16" in spark_submit["sparkSubmitParameters"]


def test_clinvar_uses_default_tuning():
    operator = build_import_operator("clinvar")
    spark_submit = operator.job_driver["sparkSubmit"]

    assert operator.waiter_max_attempts == 480
    assert "spark.dynamicAllocation.maxExecutors=4" in spark_submit["sparkSubmitParameters"]


def test_unmapped_source_raises():
    with pytest.raises(AirflowException, match="No Spark command mapped"):
        _spark_command_for("does-not-exist")
