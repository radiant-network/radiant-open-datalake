import os
from unittest.mock import MagicMock, patch

import pytest

from dags.lib.operators.ecs import PythonScriptOperator


class DummyECSEnv:
    ECS_CLUSTER = "test-cluster"
    ECS_SUBNETS = ["subnet-123"]
    ECS_SECURITY_GROUPS = ["sg-123"]


@pytest.fixture
def mock_config():
    with patch("dags.lib.operators.ecs.config") as mock_config:
        mock_config.container_name = "container"
        mock_config.ecs_env = DummyECSEnv()
        mock_config.ecs_task_definition = "task-def"
        mock_config.awslogs_group = "log-group"
        mock_config.awslogs_region = "us-east-1"
        mock_config.awslogs_stream_prefix = "prefix"
        mock_config.s3_conn_id = "mys3"
        yield mock_config


def test_python_script_operator_command_and_env(mock_config):
    script_name = "myscript.py"
    script_args = {"foo": "bar", "num": 42}
    with (
        patch.dict(os.environ, {"AIRFLOW_CONN_MYS3_TEST": "val1", "OTHER": "val2"}),
        patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done") as mock_super_execute,
    ):
        op = PythonScriptOperator(task_id="test_task", script_name=script_name, script_args=script_args)
        context = MagicMock()
        result = op.execute(context)

        expected_command = ["python", "myscript.py", "--foo", "bar", "--num", "42"]
        container_overrides = op.overrides["containerOverrides"][0]
        assert container_overrides["command"] == expected_command
        assert len(container_overrides["environment"]) == 1
        assert container_overrides["environment"][0]["name"] == "AIRFLOW_CONN_MYS3_TEST"
        assert container_overrides["environment"][0]["value"] == "val1"

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)

        assert "script_args" in PythonScriptOperator.template_fields
        assert "script_name" in PythonScriptOperator.template_fields
