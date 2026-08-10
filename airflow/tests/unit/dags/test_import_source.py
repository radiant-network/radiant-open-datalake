import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from opendatalake.dags.import_source import build_import_operator
from opendatalake.lib.domain.model.sources import get_import_config
from opendatalake.lib.operators.spark_k8s import DEFAULT_ENTRY_CLASS, JAR_PATH


def _build_operator(source_id: str):
    with DAG(dag_id="test_import", schedule=None):
        version = EmptyOperator(task_id="get_version").output
        return build_import_operator(source_id, version), version


def _conf_value(args: list, key: str) -> str | None:
    """Return the value of a `--conf key=value` flag in a spark-submit arg list, or None."""
    for i, arg in enumerate(args):
        if arg == "--conf" and args[i + 1].startswith(f"{key}="):
            return args[i + 1].split("=", 1)[1]
    return None


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_clinvar", "opendatalake_import"}


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


def test_manual_source_import_dag_exists(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-1000_genomes")
    assert dag is not None
    assert not dag_bag.import_errors
    assert "version" in dag.params


def test_import_dag_has_version_param(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-import-clinvar")
    assert "version" in dag.params


def test_entry_point_arguments():
    operator, version = _build_operator("clinvar")
    args = operator.arguments

    # spark-submit is the pod entrypoint; the ETL command/args follow the app JAR.
    assert operator.cmds == ["/opt/spark/bin/spark-submit"]
    jar_at = args.index(JAR_PATH)
    assert args[jar_at - 2 : jar_at] == ["--class", DEFAULT_ENTRY_CLASS]
    assert "local[*]" in args  # --master local[*]

    entry_args = args[jar_at + 1 :]
    version_at = entry_args.index("--version")
    assert entry_args[version_at + 1] is version
    assert entry_args[:version_at] == ["clinvar", "--config", "config/dev.conf", "--steps", "default"]
    assert entry_args[version_at + 2 :] == ["--raw-storage", "s3a://opendatalake-dev/raw/landing"]


def test_dbsnp_tuning_applied():
    operator, _ = _build_operator("dbsnp")

    # import_config.spark_conf is passed through as spark-submit --conf flags.
    assert _conf_value(operator.arguments, "spark.dynamicAllocation.maxExecutors") == "16"
    # EMR-only waiter tuning is accepted but ignored by the k8s operator.
    assert not hasattr(operator, "waiter_max_attempts")


def test_clinvar_uses_default_tuning():
    operator, _ = _build_operator("clinvar")

    # clinvar declares no spark_conf, so no executor override is emitted.
    assert _conf_value(operator.arguments, "spark.dynamicAllocation.maxExecutors") is None
    # falls back to the operator's default driver memory.
    driver_memory_at = operator.arguments.index("--driver-memory")
    assert operator.arguments[driver_memory_at + 1] == "4g"


def test_import_config_sourced_from_source_config():
    import_config = get_import_config("dbsnp")
    assert import_config.spark_command == "dbsnp"
    assert import_config.waiter_max_attempts == 960
    assert import_config.spark_conf == {"spark.dynamicAllocation.maxExecutors": "16"}


def test_unknown_source_raises():
    with pytest.raises(KeyError):
        get_import_config("does-not-exist")
