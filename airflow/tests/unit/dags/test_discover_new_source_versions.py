from unittest.mock import patch


def test_dag_loads_without_errors(dag_bag):
    with patch("dags.discover_new_source_versions.config") as config_mock:
        config_mock.dag_display_name_prefix = "Open Datalake"
        config_mock.dag_id_prefix = "opendatalake"
        config_mock.dag_default_tags = ["opendatalake"]
        dag = dag_bag.get_dag(dag_id="opendatalake-discover-new-source-versions")
        assert dag is not None
        assert not dag_bag.import_errors


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-discover-new-source-versions")
    expected_tasks = {
        "process_clinvar.check_for_update",
        "process_clinvar.should_continue",
        "process_clinvar.emit_asset_event",
    }
    assert set(dag.task_ids) == expected_tasks
