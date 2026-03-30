def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-discover-new-source-versions")
    assert dag is not None
    assert not dag_bag.import_errors


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag("opendatalake-discover-new-source-versions")
    expected_tasks = {"poll-clinvar"}
    assert set(dag.task_ids) == expected_tasks
