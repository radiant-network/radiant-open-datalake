from unittest.mock import MagicMock, patch

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from opendatalake.dags.run_sql_on_iceberg import (
    _RUN_SQL_SCRIPT,
    _SCRIPT_KEY,
    _script_s3_uri,
)
from opendatalake.lib import config
from opendatalake.lib.operators.emr import EmrServerlessConfig, EmrServerlessJobOperator

DAG_ID = "opendatalake-run-sql"


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert dag is not None
    assert not dag_bag.import_errors


def test_dag_is_manual_only(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert dag.schedule is None
    assert "opendatalake_manual" in dag.tags


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert set(dag.task_ids) == {"prepare_job", "run_sql"}


def test_run_sql_depends_on_prepare(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert dag.get_task("run_sql").upstream_task_ids == {"prepare_job"}


def test_dag_has_expected_params(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert set(dag.params) == {"query", "num_rows", "truncate"}


def _build_operator():
    with DAG(dag_id="test_run_sql", schedule=None):
        prepared = EmptyOperator(task_id="prepare_job").output
        return EmrServerlessJobOperator(
            task_id="run_sql",
            entry_point=_script_s3_uri(),
            entry_point_arguments=["--query", prepared, "--num-rows", "20", "--truncate", "0"],
            spark_conf={"spark.dynamicAllocation.maxExecutors": "2"},
        )


def test_job_runs_pyspark_entry_point():
    op = _build_operator()
    spark_submit = op.job_driver["sparkSubmit"]

    # PySpark entry point (the uploaded script), not the fat JAR, and no --class.
    assert spark_submit["entryPoint"] == _script_s3_uri()
    assert spark_submit["entryPoint"].endswith("run_sql.py")
    assert "--class" not in spark_submit["sparkSubmitParameters"]


def test_fat_jar_added_as_spark_jars_for_iceberg_classes():
    op = _build_operator()
    params = op.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    jar = EmrServerlessConfig.from_env().jar_s3_path
    # The bare PySpark runtime has no Iceberg/Glue classes; the fat JAR supplies them.
    assert f"--conf spark.jars={jar}" in params
    # Catalog conf still injected by the operator.
    assert "--conf spark.sql.catalog.opendatalake.warehouse=" in params
    assert "--conf spark.sql.defaultCatalog=opendatalake" in params


def test_query_arg_forwarded_as_xcom():
    op = _build_operator()
    args = op.job_driver["sparkSubmit"]["entryPointArguments"]
    # The query is an XComArg (resolved from prepare_job at run time), not a Jinja string.
    assert args[0] == "--query"
    assert not isinstance(args[1], str)


def test_prepare_job_rejects_empty_query():
    prepare = _prepare_callable()
    with pytest.raises(ValueError, match="query"):
        prepare(params={"query": "   "})


def test_prepare_job_uploads_script_and_stringifies_args():
    prepare = _prepare_callable()
    with patch("opendatalake.dags.run_sql_on_iceberg.S3Hook") as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        out = prepare(params={"query": "SELECT 1", "num_rows": 5, "truncate": 30})

    hook.load_string.assert_called_once_with(
        _RUN_SQL_SCRIPT,
        key=_SCRIPT_KEY,
        bucket_name=config.raw_datalake_bucket,
        replace=True,
    )
    assert out == {"query": "SELECT 1", "num_rows": "5", "truncate": "30"}


def _prepare_callable():
    """The `prepare_job` TaskFlow callable, unwrapped from the DAG for direct calling."""
    from opendatalake.dags.run_sql_on_iceberg import run_sql_on_iceberg

    dag = run_sql_on_iceberg()
    return dag.get_task("prepare_job").python_callable
