import os
from unittest.mock import MagicMock, patch

from dags.lib.operators.ecs import EcsConfig, PythonScriptOperator

TEST_ECS_CONFIG = EcsConfig(
    cluster="test-cluster",
    subnets=["subnet-1"],
    security_groups=["sg-1"],
    s3_workspace="s3://test-workspace",
    container_name="test-container",
    task_definition="test-task-def",
    awslogs_group="test-log-group",
    awslogs_region="us-east-1",
    awslogs_stream_prefix="test-prefix",
)


def test_ecs_config_from_env_reads_environment_variables():
    env = {
        "OPENDATALAKE_ECS_CLUSTER": "my-cluster",
        "OPENDATALAKE_ECS_SUBNETS": "subnet-a, subnet-b",
        "OPENDATALAKE_ECS_SECURITY_GROUPS": "sg-a,sg-b",
        "OPENDATALAKE_ECS_S3_WORKSPACE": "s3://workspace",
        "OPENDATALAKE_TASK_OPERATOR_TASK_DEFINITION": "my-task-def",
        "OPENDATALAKE_TASK_OPERATOR_LOG_GROUP": "my-log-group",
        "OPENDATALAKE_TASK_OPERATOR_LOG_REGION": "us-east-1",
        "OPENDATALAKE_TASK_OPERATOR_LOG_PREFIX": "my-prefix",
    }
    EcsConfig.from_env.cache_clear()
    try:
        with patch.dict(os.environ, env):
            cfg = EcsConfig.from_env()

        assert cfg.cluster == "my-cluster"
        assert cfg.subnets == ["subnet-a", "subnet-b"]
        assert cfg.security_groups == ["sg-a", "sg-b"]
        assert cfg.s3_workspace == "s3://workspace"
        assert cfg.task_definition == "my-task-def"
        assert cfg.awslogs_group == "my-log-group"
        assert cfg.awslogs_region == "us-east-1"
        assert cfg.awslogs_stream_prefix == "my-prefix"
    finally:
        EcsConfig.from_env.cache_clear()


def test_python_script_operator_inject_command_correctly():
    script_name = "myscript.py"
    script_args = {"foo": "bar", "num": 42}
    with patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done") as mock_super_execute:
        op = PythonScriptOperator(
            task_id="test_task", script_name=script_name, script_args=script_args, ecs_config=TEST_ECS_CONFIG
        )
        context = MagicMock()
        result = op.execute(context)

        expected_command = ["python", "myscript.py", "--foo", "bar", "--num", "42"]
        container_overrides = op.overrides["containerOverrides"][0]
        assert container_overrides["command"] == expected_command

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)

        assert "script_args" in PythonScriptOperator.template_fields
        assert "script_name" in PythonScriptOperator.template_fields


def test_python_script_operator_uses_injected_config():
    with patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"):
        op = PythonScriptOperator(
            task_id="test_task", script_name="myscript.py", script_args={}, ecs_config=TEST_ECS_CONFIG
        )

        assert op.cluster == "test-cluster"
        assert op.task_definition == "test-task-def"
        assert op.container_name == "test-container"
        assert op.network_configuration["awsvpcConfiguration"]["subnets"] == ["subnet-1"]
        assert op.network_configuration["awsvpcConfiguration"]["securityGroups"] == ["sg-1"]


def test_python_script_operator_appends_to_user_container_overrides():
    user_overrides = {"containerOverrides": [{"name": "user-container", "command": ["echo", "hi"]}]}
    script_name = "myscript.py"
    script_args = {"foo": "bar"}

    with patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done") as mock_super_execute:
        op = PythonScriptOperator(
            task_id="test_task",
            script_name=script_name,
            script_args=script_args,
            overrides=user_overrides,
            ecs_config=TEST_ECS_CONFIG,
        )
        context = MagicMock()
        result = op.execute(context)

        # The operator should append its own override to the list
        container_overrides = op.overrides["containerOverrides"]
        assert len(container_overrides) == 2
        assert container_overrides[0] == {"name": "user-container", "command": ["echo", "hi"]}
        assert container_overrides[1] == {
            "name": TEST_ECS_CONFIG.container_name,
            "command": ["python", "myscript.py", "--foo", "bar"],
        }

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)
