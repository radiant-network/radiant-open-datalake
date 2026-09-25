import sys
import types
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from opendatalake.dags.tag_latest_on_iceberg import (
    _SCRIPT_KEY,
    _TAG_LATEST_SCRIPT,
    _script_s3_uri,
)
from opendatalake.lib import config
from opendatalake.lib.operators.emr import EmrServerlessConfig, EmrServerlessJobOperator

DAG_ID = "opendatalake-tag-latest"


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
    assert set(dag.task_ids) == {"prepare_job", "tag_latest"}


def test_tag_latest_depends_on_prepare(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert dag.get_task("tag_latest").upstream_task_ids == {"prepare_job"}


def test_dag_has_expected_params(dag_bag):
    dag = dag_bag.get_dag(dag_id=DAG_ID)
    assert set(dag.params) == {"database", "tables", "tag", "dry_run"}


def _build_operator():
    with DAG(dag_id="test_tag_latest", schedule=None):
        prepared = EmptyOperator(task_id="prepare_job").output
        return EmrServerlessJobOperator(
            task_id="tag_latest",
            entry_point=_script_s3_uri(),
            entry_point_arguments=["--database", prepared, "--tables", "", "--tag", "latest", "--dry-run", "false"],
            spark_conf={"spark.dynamicAllocation.maxExecutors": "2"},
        )


def test_job_runs_pyspark_entry_point():
    op = _build_operator()
    spark_submit = op.job_driver["sparkSubmit"]

    assert spark_submit["entryPoint"] == _script_s3_uri()
    assert spark_submit["entryPoint"].endswith("tag_latest.py")
    assert "--class" not in spark_submit["sparkSubmitParameters"]


def test_fat_jar_added_as_spark_jars_for_iceberg_classes():
    op = _build_operator()
    params = op.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    jar = EmrServerlessConfig.from_env().jar_s3_path

    assert f"--conf spark.jars={jar}" in params
    assert "--conf spark.sql.defaultCatalog=opendatalake" in params


def test_prepare_job_rejects_empty_database():
    prepare = _prepare_callable()
    with pytest.raises(ValueError, match="database"):
        prepare(params={"database": "   "})


def test_prepare_job_uploads_script_and_stringifies_args():
    prepare = _prepare_callable()
    with patch("opendatalake.dags.tag_latest_on_iceberg.S3Hook") as hook_cls:
        hook = MagicMock()
        hook_cls.return_value = hook
        out = prepare(params={"database": "reference", "tables": " clinvar_v1 , dbsnp_v1 ", "dry_run": True})

    hook.load_string.assert_called_once_with(
        _TAG_LATEST_SCRIPT,
        key=_SCRIPT_KEY,
        bucket_name=config.raw_datalake_bucket,
        replace=True,
    )
    assert out == {
        "database": "reference",
        "tables": "clinvar_v1,dbsnp_v1",
        "tag": "latest",
        "dry_run": "true",
    }


def _prepare_callable():
    """The `prepare_job` TaskFlow callable, unwrapped from the DAG for direct calling."""
    from opendatalake.dags.tag_latest_on_iceberg import tag_latest_on_iceberg

    dag = tag_latest_on_iceberg()
    return dag.get_task("prepare_job").python_callable


# --- The uploaded PySpark script ------------------------------------------------------------------
# It ships as a string, so exec it into a namespace to exercise its selection rules. pyspark is not a
# dependency of the Airflow project, hence the stub module.


@pytest.fixture(scope="module")
def script():
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = MagicMock()
    modules = {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": pyspark_sql}
    namespace = {"__name__": "tag_latest_script"}
    with patch.dict(sys.modules, modules):
        exec(_TAG_LATEST_SCRIPT, namespace)  # noqa: S102
    return SimpleNamespace(**namespace)


class _FakeSpark:
    """Answers `sql()` from the first response whose key appears in the query."""

    def __init__(self, responses):
        self.responses = responses
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        rows = next((rows for fragment, rows in self.responses.items() if fragment in query), [])
        return SimpleNamespace(collect=lambda: rows)


def test_version_branches_ignores_main_and_audit(script):
    spark = _FakeSpark(
        {
            ".refs": [
                {"name": "main", "snapshot_id": 1},
                {"name": "audit_20260715", "snapshot_id": 2},
                {"name": "20260715", "snapshot_id": 3},
            ]
        }
    )

    assert script.version_branches(spark, "reference.clinvar_v1") == [("20260715", 3)]
    # Metadata is cached with a 30s TTL; the ref read must be preceded by a refresh.
    assert spark.queries[0] == "REFRESH TABLE reference.clinvar_v1"


def test_newest_branch_with_data_picks_most_recent_non_empty(script):
    index = {
        3: (datetime(2026, 7, 15), "10"),
        4: (datetime(2026, 9, 1), "0"),  # published later, but empty
        5: (datetime(2026, 8, 1), "42"),
    }
    branches = [("20260715", 3), ("20260901", 4), ("20260801", 5)]

    name, snapshot_id, committed_at = script.newest_branch_with_data(None, "reference.clinvar_v1", branches, index)

    assert (name, snapshot_id, committed_at) == ("20260801", 5, datetime(2026, 8, 1))


def test_newest_branch_with_data_returns_none_when_all_empty(script):
    index = {3: (datetime(2026, 7, 15), "0")}

    assert script.newest_branch_with_data(None, "reference.clinvar_v1", [("20260715", 3)], index) is None


def test_newest_branch_with_data_counts_rows_when_summary_lacks_total_records(script):
    spark = MagicMock()
    spark.read.option.return_value.table.return_value.count.return_value = 7
    index = {3: (datetime(2026, 7, 15), None)}

    picked = script.newest_branch_with_data(spark, "reference.clinvar_v1", [("20260715", 3)], index)

    assert picked == ("20260715", 3, datetime(2026, 7, 15))
    spark.read.option.assert_called_once_with("branch", "20260715")


def test_tag_snapshot_quotes_ref_and_verifies_it_moved(script):
    spark = _FakeSpark({"WHERE name = 'latest'": [(99,)]})

    script.tag_snapshot(spark, "reference.clinvar_v1", "latest", 99)

    assert "ALTER TABLE reference.clinvar_v1 CREATE OR REPLACE TAG `latest` AS OF VERSION 99" in spark.queries


def test_tag_snapshot_fails_when_ref_did_not_move(script):
    spark = _FakeSpark({"WHERE name = 'latest'": [(1,)]})

    with pytest.raises(RuntimeError, match="did not land"):
        script.tag_snapshot(spark, "reference.clinvar_v1", "latest", 99)


def test_process_dry_run_writes_nothing(script):
    spark = _FakeSpark(
        {
            ".refs": [{"name": "20260715", "snapshot_id": 3}],
            ".snapshots": [
                {"snapshot_id": 3, "committed_at": datetime(2026, 7, 15), "summary": {"total-records": "9"}}
            ],
        }
    )

    assert script.process(spark, "reference", "clinvar_v1", "latest", dry_run=True) == "planned"
    assert not [q for q in spark.queries if "ALTER TABLE" in q]


def test_process_skips_table_without_version_branch(script):
    spark = _FakeSpark({".refs": [{"name": "main", "snapshot_id": 1}]})

    assert script.process(spark, "reference", "clinvar_v1", "latest", dry_run=False) == "skipped"
