import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from opendatalake.lib import config
from opendatalake.lib.operators.ecs import EcsConfig, PythonScriptOperator

TEST_ECS_CONFIG = EcsConfig(
    cluster="test-cluster",
    subnets=("subnet-1",),
    security_groups=("sg-1",),
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
        assert cfg.subnets == ("subnet-a", "subnet-b")
        assert cfg.security_groups == ("sg-a", "sg-b")
        assert cfg.task_definition == "my-task-def"
        assert cfg.awslogs_group == "my-log-group"
        assert cfg.awslogs_region == "us-east-1"
        assert cfg.awslogs_stream_prefix == "my-prefix"
        assert cfg.missing_required() == {}
    finally:
        EcsConfig.from_env.cache_clear()


def test_python_script_operator_fails_at_construction_on_incomplete_config():
    incomplete_config = EcsConfig(
        cluster="",
        subnets=(),
        security_groups=("sg-1",),
        container_name="test-container",
        task_definition=None,
        awslogs_group=None,
        awslogs_region=None,
        awslogs_stream_prefix=None,
    )

    with pytest.raises(AirflowException, match="Incomplete ECS configuration") as exc_info:
        PythonScriptOperator(
            task_id="test_task", script_name="myscript.py", script_args={}, ecs_config=incomplete_config
        )

    message = str(exc_info.value)
    assert "cluster" in message
    assert "OPENDATALAKE_ECS_CLUSTER" in message
    assert "OPENDATALAKE_ECS_SUBNETS" in message
    assert "OPENDATALAKE_TASK_OPERATOR_TASK_DEFINITION" in message
    assert "OPENDATALAKE_ECS_SECURITY_GROUPS" not in message


def test_python_script_operator_inject_command_correctly():
    script_name = "myscript.py"
    script_args = {"foo": "bar", "num": 42}
    with patch(
        "opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"
    ) as mock_super_execute:
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


def test_python_script_operator_injects_raw_storage_env():
    with patch("opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"):
        op = PythonScriptOperator(
            task_id="test_task", script_name="myscript.py", script_args={}, ecs_config=TEST_ECS_CONFIG
        )
        op.execute(MagicMock())

    env = op.overrides["containerOverrides"][0]["environment"]
    # ECS requires {name, value} pairs inside the container override, not {key: value} at task level.
    assert env == [
        {"name": "OPENDATALAKE_RAW_BUCKET", "value": config.raw_datalake_bucket},
        {"name": "OPENDATALAKE_RAW_LANDING_ROOT", "value": config.raw_landing_root},
    ]


def test_python_script_operator_uses_injected_config():
    with patch("opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"):
        op = PythonScriptOperator(
            task_id="test_task", script_name="myscript.py", script_args={}, ecs_config=TEST_ECS_CONFIG
        )

        assert op.cluster == "test-cluster"
        assert op.task_definition == "test-task-def"
        assert op.container_name == "test-container"
        assert op.network_configuration["awsvpcConfiguration"]["subnets"] == ["subnet-1"]
        assert op.network_configuration["awsvpcConfiguration"]["securityGroups"] == ["sg-1"]


def test_python_script_operator_injects_secrets_by_arn_into_the_container():
    with (
        patch("opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"),
        patch.dict(os.environ, {"TOKEN_ARN": "arn:aws:secretsmanager:...:secret:token"}),
    ):
        op = PythonScriptOperator(
            task_id="test_task",
            script_name="myscript.py",
            script_args={},
            ecs_config=TEST_ECS_CONFIG,
            secret_env_vars=(("TOKEN", "TOKEN_ARN"),),
        )
        op.execute(MagicMock())

        override = op.overrides["containerOverrides"][0]
        # valueFrom carries the ARN, not the secret value — nothing sensitive in the RunTask call.
        assert override["secrets"] == [{"name": "TOKEN", "valueFrom": "arn:aws:secretsmanager:...:secret:token"}]
        # Raw-storage env is always injected (secrets and environment coexist).
        assert {"name": "OPENDATALAKE_RAW_BUCKET", "value": config.raw_datalake_bucket} in override["environment"]


def test_python_script_operator_omits_secrets_when_none_declared():
    with patch("opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"):
        op = PythonScriptOperator(
            task_id="test_task", script_name="myscript.py", script_args={}, ecs_config=TEST_ECS_CONFIG
        )
        op.execute(MagicMock())

        assert "secrets" not in op.overrides["containerOverrides"][0]


def test_python_script_operator_skips_secret_when_arn_unset():
    # No ARN in the worker environment -> no secret injected; the download then fails loudly on its own
    # auth check inside the container.
    with (
        patch("opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"),
        patch.dict(os.environ, {}, clear=False),
    ):
        os.environ.pop("DEFINITELY_UNSET_ARN", None)
        op = PythonScriptOperator(
            task_id="test_task",
            script_name="myscript.py",
            script_args={},
            ecs_config=TEST_ECS_CONFIG,
            secret_env_vars=(("TOKEN", "DEFINITELY_UNSET_ARN"),),
        )
        op.execute(MagicMock())

        assert "secrets" not in op.overrides["containerOverrides"][0]


def test_python_script_operator_appends_to_user_container_overrides():
    user_overrides = {"containerOverrides": [{"name": "user-container", "command": ["echo", "hi"]}]}
    script_name = "myscript.py"
    script_args = {"foo": "bar"}

    with patch(
        "opendatalake.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done"
    ) as mock_super_execute:
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
            "environment": [
                {"name": "OPENDATALAKE_RAW_BUCKET", "value": config.raw_datalake_bucket},
                {"name": "OPENDATALAKE_RAW_LANDING_ROOT", "value": config.raw_landing_root},
            ],
        }

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)
