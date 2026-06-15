import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from dags.lib.operators.emr import (
    DEFAULT_ENTRY_CLASS,
    EmrServerlessConfig,
    EmrServerlessJobOperator,
)

TEST_EMR_CONFIG = EmrServerlessConfig(
    application_id="app-123",
    execution_role_arn="arn:aws:iam::123456789012:role/service-role/EMR-Exec",
    jar_s3_path="s3://bucket/jars/radiant-open-datalake-spark.jar",
    warehouse_s3="s3://bucket/opendatalake/",
    glue_catalog_id="123456789012",
    region="us-east-1",
    glue_database="opendatalake_test",
    cloudwatch_log_group="/aws/emr-serverless/opendatalake",
    cloudwatch_log_stream_prefix="test_prefix",
    cloudwatch_region=None,
)


def _operator(**overrides):
    kwargs = {
        "task_id": "test_task",
        "entry_point_arguments": ["clinvar", "--steps", "default"],
        "emr_config": TEST_EMR_CONFIG,
    }
    kwargs.update(overrides)
    return EmrServerlessJobOperator(**kwargs)


def test_emr_config_from_env_reads_environment_variables():
    env = {
        "OPENDATALAKE_EMR_APPLICATION_ID": "app-abc",
        "OPENDATALAKE_EMR_EXECUTION_ROLE_ARN": "arn:aws:iam::000:role/r",
        "OPENDATALAKE_EMR_JAR_S3_PATH": "s3://b/j.jar",
        "OPENDATALAKE_EMR_WAREHOUSE_S3": "s3://b/wh/",
        "OPENDATALAKE_EMR_GLUE_CATALOG_ID": "000111222333",
        "OPENDATALAKE_EMR_REGION": "us-west-2",
        "OPENDATALAKE_EMR_GLUE_DATABASE": "opendatalake_prod",
        "OPENDATALAKE_EMR_LOG_GROUP": "/aws/emr/og",
        "OPENDATALAKE_EMR_LOG_PREFIX": "og_prefix",
        "OPENDATALAKE_EMR_LOG_REGION": "us-west-1",
    }
    EmrServerlessConfig.from_env.cache_clear()
    try:
        with patch.dict(os.environ, env):
            cfg = EmrServerlessConfig.from_env()

        assert cfg.application_id == "app-abc"
        assert cfg.execution_role_arn == "arn:aws:iam::000:role/r"
        assert cfg.jar_s3_path == "s3://b/j.jar"
        assert cfg.warehouse_s3 == "s3://b/wh/"
        assert cfg.glue_catalog_id == "000111222333"
        assert cfg.region == "us-west-2"
        assert cfg.glue_database == "opendatalake_prod"
        assert cfg.cloudwatch_log_group == "/aws/emr/og"
        assert cfg.cloudwatch_log_stream_prefix == "og_prefix"
        assert cfg.cloudwatch_region == "us-west-1"
        assert cfg.missing_required() == {}
    finally:
        EmrServerlessConfig.from_env.cache_clear()


def test_operator_fails_at_construction_on_incomplete_config():
    incomplete_config = EmrServerlessConfig(
        application_id="",
        execution_role_arn="arn:aws:iam::000:role/r",
        jar_s3_path="s3://b/j.jar",
        warehouse_s3="",
        glue_catalog_id="000",
        region="us-east-1",
        glue_database="db",
        cloudwatch_log_group="/lg",
        cloudwatch_log_stream_prefix=None,
        cloudwatch_region=None,
    )

    with pytest.raises(AirflowException, match="Incomplete EMR Serverless configuration") as exc_info:
        _operator(emr_config=incomplete_config)

    message = str(exc_info.value)
    assert "application_id" in message
    assert "OPENDATALAKE_EMR_APPLICATION_ID" in message
    assert "OPENDATALAKE_EMR_WAREHOUSE_S3" in message
    # A populated field's env var must not be reported as missing.
    assert "OPENDATALAKE_EMR_JAR_S3_PATH" not in message


def test_job_driver_construction_uses_config_and_default_entry_class():
    op = _operator()

    spark_submit = op.job_driver["sparkSubmit"]
    assert spark_submit["entryPoint"] == TEST_EMR_CONFIG.jar_s3_path
    assert spark_submit["entryPointArguments"] == ["clinvar", "--steps", "default"]

    params = spark_submit["sparkSubmitParameters"]
    assert f"--class {DEFAULT_ENTRY_CLASS}" in params
    assert f"--conf spark.sql.catalog.opendatalake.warehouse={TEST_EMR_CONFIG.warehouse_s3}" in params
    assert f"--conf spark.sql.catalog.opendatalake.glue.id={TEST_EMR_CONFIG.glue_catalog_id}" in params
    assert f"--conf spark.sql.catalog.opendatalake.default-namespace={TEST_EMR_CONFIG.glue_database}" in params
    assert f"--conf spark.sql.catalog.opendatalake.client.region={TEST_EMR_CONFIG.region}" in params


def test_custom_entry_class():
    op = _operator(entry_class="org.example.OtherJob")
    assert "--class org.example.OtherJob" in op.job_driver["sparkSubmit"]["sparkSubmitParameters"]


def test_spark_conf_merge_and_override():
    op = _operator(spark_conf={"spark.sql.shuffle.partitions": "200", "spark.custom.flag": "x"})

    params = op.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    assert "--conf spark.sql.shuffle.partitions=200" in params  # caller override wins
    assert "--conf spark.sql.shuffle.partitions=16" not in params  # base default replaced
    assert "--conf spark.custom.flag=x" in params  # addition
    assert "--conf spark.sql.catalog.opendatalake.warehouse=" in params  # catalog block intact


def test_conf_value_with_spaces_is_quoted():
    op = _operator(spark_conf={"spark.executor.extraJavaOptions": "-Dx=1 -Dy=2"})
    params = op.job_driver["sparkSubmit"]["sparkSubmitParameters"]
    # Must be quoted as one token, else spark-submit splits on the space.
    assert '--conf "spark.executor.extraJavaOptions=-Dx=1 -Dy=2"' in params
    # Space-free confs stay unquoted.
    assert "--conf spark.sql.shuffle.partitions=16" in params


def test_uses_injected_config():
    op = _operator()

    assert op.application_id == TEST_EMR_CONFIG.application_id
    assert op.execution_role_arn == TEST_EMR_CONFIG.execution_role_arn
    assert op.cloudwatch_log_group == TEST_EMR_CONFIG.cloudwatch_log_group

    cw = op.configuration_overrides["monitoringConfiguration"]["cloudWatchLoggingConfiguration"]
    assert cw["enabled"] is True
    assert cw["logGroupName"] == TEST_EMR_CONFIG.cloudwatch_log_group
    assert cw["logStreamNamePrefix"] == TEST_EMR_CONFIG.cloudwatch_log_stream_prefix


def test_defaults_to_deferrable_with_long_timeout():
    op = _operator()
    assert op.deferrable is True
    assert op.waiter_delay == 60
    assert op.waiter_max_attempts == 480  # ~8h defer timeout


def test_deferrable_can_be_disabled():
    op = _operator(deferrable=False)
    assert op.deferrable is False


def test_execute_complete_forwards_driver_logs_from_event():
    class _ResourceNotFound(Exception):
        pass

    hook = MagicMock()
    hook.get_conn.return_value.exceptions.ResourceNotFoundException = _ResourceNotFound
    hook.get_log_events.return_value = []

    event = {"status": "success", "job_details": {"job_id": "job-xyz", "application_id": "app-123"}}

    with (
        patch("dags.lib.operators.emr.AwsLogsHook", return_value=hook),
        patch(
            "dags.lib.operators.emr.EmrServerlessStartJobOperator.execute_complete",
            return_value="job-xyz",
        ) as mock_super,
    ):
        op = _operator()
        result = op.execute_complete(MagicMock(), event)

    assert result == "job-xyz"
    mock_super.assert_called_once()
    calls = hook.get_log_events.call_args_list
    streams = [c.kwargs["log_stream_name"] for c in calls]
    # job_id comes from the event (self.job_id is not restored across deferral).
    assert any("job-xyz" in s and "SPARK_DRIVER" in s for s in streams)
    assert all(c.kwargs["log_group"] == TEST_EMR_CONFIG.cloudwatch_log_group for c in calls)


def test_execute_complete_does_not_mask_result_on_log_forwarding_failure():
    event = {"status": "success", "job_details": {"job_id": "job-xyz", "application_id": "app-123"}}
    with (
        patch("dags.lib.operators.emr.AwsLogsHook", side_effect=Exception("cloudwatch down")),
        patch(
            "dags.lib.operators.emr.EmrServerlessStartJobOperator.execute_complete",
            return_value="job-xyz",
        ),
    ):
        op = _operator()
        result = op.execute_complete(MagicMock(), event)
    # Log-forwarding error must be swallowed; the job's result stands.
    assert result == "job-xyz"


def test_sync_path_forwards_logs_in_finally():
    with patch("dags.lib.operators.emr.EmrServerlessStartJobOperator.execute", return_value="job-1"):
        op = _operator(deferrable=False)
        with patch.object(op, "_forward_driver_logs") as fwd:
            op.execute(MagicMock())
        fwd.assert_called_once()


def test_deferrable_requires_wait_for_completion():
    with pytest.raises(AirflowException, match="wait_for_completion"):
        _operator(wait_for_completion=False)  # deferrable defaults True


def test_deferrable_rejects_wait_for_completion_none():
    with pytest.raises(AirflowException, match="wait_for_completion"):
        _operator(wait_for_completion=None)  # None is falsy → also fire-and-forget


def test_region_name_pinned_to_config():
    op = _operator()
    assert op.region_name == TEST_EMR_CONFIG.region


@pytest.mark.parametrize("reserved", ["region_name", "application_id", "execution_role_arn", "job_driver"])
def test_operator_managed_kwargs_are_rejected(reserved):
    with pytest.raises(AirflowException, match="managed by EmrServerlessJobOperator"):
        _operator(**{reserved: "x"})


def test_cloudwatch_region_defaults_to_region():
    op = _operator()
    assert op.cloudwatch_region == TEST_EMR_CONFIG.region


def test_template_fields():
    tf = EmrServerlessJobOperator.template_fields
    assert "name" in tf
    assert "job_driver" in tf
    assert "cloudwatch_log_group" in tf
    assert "cloudwatch_log_stream_prefix" in tf
