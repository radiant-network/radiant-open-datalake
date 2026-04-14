from unittest.mock import patch

from dags.download_source import direct_upload
from dags.lib.domain.model.config import DownloadConfig


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_download", "opendatalake_clinvar"}


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    expected_tasks = ["get_version", "get_prefix", "download_files.1_local_upload_vcf", "finalize_download"]
    assert expected_tasks == dag.task_ids


def test_direct_upload_task_calls_download():
    download_config = DownloadConfig(download_url="http://example.com/file.txt", use_direct_upload=True)

    with (
        patch("dags.download_source.download.direct_upload") as mock_direct_upload,
        patch("dags.download_source.get_download_config", return_value=download_config) as mock_get_download_config,
    ):
        direct_upload.function("test_source", "test_prefix", "test_version", 0)
        mock_direct_upload.assert_called_once_with(
            s3_prefix="test_prefix", version="test_version", download_conf=download_config
        )
        mock_get_download_config.assert_called_once_with("test_source", 0)


def test_upload_via_local_copy_task_build_python_script_operator():
    from dags.download_source import upload_via_local_copy

    operator = upload_via_local_copy(
        task_id="test_task",
        source="test_source",
        prefix="test_prefix",
        version="test_version",
        download_index=0,
    )
    assert operator.task_id == "test_task"
    assert operator.script_name == "/opt/opendatalake/upload_via_local_copy.py"
    assert operator.script_args == {
        "source": "test_source",
        "prefix": "test_prefix",
        "version": "test_version",
        "download_index": 0,
    }
