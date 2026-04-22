from unittest.mock import MagicMock, patch

from dags.download_source import direct_upload


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_download", "opendatalake_clinvar"}


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    expected_tasks = ["get_version", "get_prefix", "download_files.1_local_upload_vcf", "finalize_download"]
    assert expected_tasks == dag.task_ids


def test_direct_upload_calls_s3_downloader():
    source = "clinvar"
    prefix = "raw/clinvar/v1"
    version = "v1"
    download_index = 0

    fake_download_conf = MagicMock()
    fake_downloader = MagicMock()
    with (
        patch(
            "dags.download_source.get_download_config_at_index", return_value=fake_download_conf
        ) as mock_get_download_conf,
        patch("dags.download_source.S3Downloader", return_value=fake_downloader) as mock_downloader_constructor,
    ):
        direct_upload.function(source, prefix, version, download_index)

        mock_get_download_conf.assert_called_once_with(source, download_index)
        mock_downloader_constructor.assert_called_once_with(
            s3_prefix=prefix, version=version, download_conf=fake_download_conf
        )
        fake_downloader.direct_upload.assert_called_once()


def test_upload_via_local_copy_task_build_python_script_operator():
    from dags.download_source import upload_via_local_copy

    operator = upload_via_local_copy(
        task_id="test_task",
        source="test_source",
        prefix="test_prefix",
        version="test_version",
        download_index=0,
        label="test_label",
    )
    assert operator.task_id == "test_task"
    assert operator.script_name == "/opt/opendatalake/upload_via_local_copy.py"
    assert operator.script_args == {
        "source": "test_source",
        "prefix": "test_prefix",
        "version": "test_version",
        "download_index": 0,
    }
    assert "test_label" in operator.task_display_name
