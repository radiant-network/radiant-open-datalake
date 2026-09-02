from unittest.mock import MagicMock, patch

from airflow.timetables.simple import AssetTriggeredTimetable, NullTimetable

from opendatalake.dags.download_source import direct_upload, stream_unzip_download


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert dag is not None
    assert not dag_bag.import_errors
    assert dag.tags == {"opendatalake", "opendatalake_download", "opendatalake_clinvar", "opendatalake_auto"}


def test_auto_source_download_dag_is_asset_scheduled_with_version_param(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert isinstance(dag.timetable, AssetTriggeredTimetable)
    assert "version" in dag.params
    assert "opendatalake_auto" in dag.tags


def test_manual_source_download_dag_is_trigger_only(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-1000_genomes")
    assert dag is not None
    assert not dag_bag.import_errors
    assert isinstance(dag.timetable, NullTimetable)
    assert "version" in dag.params
    assert "opendatalake_manual" in dag.tags


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    expected_tasks = ["get_version", "get_prefix", "download_files.1_local_upload_vcf", "finalize_download"]
    assert expected_tasks == dag.task_ids


def test_manual_url_source_download_dag_stream_unzips(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-dbnsfp")
    assert dag is not None
    assert not dag_bag.import_errors
    # Manual source: no schedule, triggered by hand with a pasted URL.
    assert isinstance(dag.timetable, NullTimetable)
    assert "version" in dag.params
    assert "download_url" in dag.params
    assert "opendatalake_manual" in dag.tags
    assert "download_files.1_stream_unzip_variant" in dag.task_ids


def test_fixed_url_source_download_dag_has_no_download_url_param(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert "download_url" not in dag.params


def test_manual_secret_backed_source_download_dag(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-omim")
    assert dag is not None
    assert not dag_bag.import_errors
    # Manual source: no schedule, triggered by hand with a version. The download key's ARN is forwarded
    # to the container by the operator (secret_arn_env_vars), so there is no key/ARN param on the DAG.
    assert isinstance(dag.timetable, NullTimetable)
    assert "version" in dag.params
    assert "download_url" not in dag.params
    assert "opendatalake_manual" in dag.tags
    assert "download_files.1_local_upload_tsv" in dag.task_ids


def test_secret_backed_local_copy_does_not_pass_the_key_as_a_script_arg():
    from opendatalake.dags.download_source import upload_via_local_copy

    secret_arn_env_vars = ("OPENDATALAKE_OMIM_DOWNLOAD_KEY_ARN",)
    operator = upload_via_local_copy(
        task_id="test_task",
        source="omim",
        prefix="raw/landing/omim/2026_08_12",
        version="2026_08_12",
        download_index=0,
        label="tsv",
        secret_arn_env_vars=secret_arn_env_vars,
    )
    assert set(operator.script_args) == {"source", "prefix", "version", "download_index"}


def test_auto_multi_file_source_download_dag(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-orphanet")
    assert dag is not None
    assert not dag_bag.import_errors
    assert isinstance(dag.timetable, AssetTriggeredTimetable)
    assert "download_url" not in dag.params
    assert "opendatalake_auto" in dag.tags
    assert "download_files.1_local_upload_xml" in dag.task_ids
    assert "download_files.2_local_upload_xml" in dag.task_ids


def test_manual_source_get_version_has_no_asset_inlet(dag_bag):
    # MANUAL sources: the input asset is inactive (never produced/scheduled), so get_version must
    # not declare it as an inlet -- otherwise AirflowInactiveAssetInInletOrOutletException at runtime.
    dag = dag_bag.get_dag(dag_id="opendatalake-download-dbnsfp")
    assert dag.get_task("get_version").inlets == []


def test_auto_source_get_version_has_asset_inlet(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    assert dag.get_task("get_version").inlets  # asset-scheduled: inlet present


def test_stream_unzip_download_builds_container_operator_with_templated_url():
    # The transfer runs in the task-operator container, not the worker; the URL is rendered from the
    # `download_url` param at runtime (templated script arg).
    operator = stream_unzip_download(
        task_id="test_task",
        source="dbnsfp",
        prefix="raw/landing/dbnsfp/4.9a",
        version="4.9a",
        download_index=0,
        label="variant",
    )
    assert operator.task_id == "test_task"
    assert operator.script_name == "/opt/opendatalake/stream_unzip_download.py"
    assert operator.script_args == {
        "source": "dbnsfp",
        "prefix": "raw/landing/dbnsfp/4.9a",
        "version": "4.9a",
        "download_index": 0,
        "download_url": "{{ params.download_url }}",
    }
    assert "variant" in operator.task_display_name


def test_direct_upload_calls_s3_downloader():
    source = "clinvar"
    prefix = "raw/clinvar/v1"
    version = "v1"
    download_index = 0

    fake_download_conf = MagicMock()
    fake_downloader = MagicMock()
    with (
        patch(
            "opendatalake.dags.download_source.get_download_config_at_index", return_value=fake_download_conf
        ) as mock_get_download_conf,
        patch(
            "opendatalake.dags.download_source.S3Downloader", return_value=fake_downloader
        ) as mock_downloader_constructor,
    ):
        direct_upload.function(source, prefix, version, download_index)

        mock_get_download_conf.assert_called_once_with(source, download_index)
        mock_downloader_constructor.assert_called_once_with(
            s3_prefix=prefix, version=version, download_conf=fake_download_conf
        )
        fake_downloader.direct_upload.assert_called_once()


def test_get_prefix_uses_raw_landing(dag_bag):
    dag = dag_bag.get_dag(dag_id="opendatalake-download-clinvar")
    get_prefix = dag.get_task("get_prefix").python_callable
    assert get_prefix("v1") == "raw/landing/clinvar/v1"


def test_upload_via_local_copy_task_build_python_script_operator():
    from opendatalake.dags.download_source import upload_via_local_copy

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
