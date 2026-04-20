from unittest.mock import MagicMock, patch

from dags.lib import config
from dags.lib.operators.ecs import PythonScriptOperator


def test_python_script_operator_inject_command_correctly():
    script_name = "myscript.py"
    script_args = {"foo": "bar", "num": 42}
    with patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done") as mock_super_execute:
        op = PythonScriptOperator(task_id="test_task", script_name=script_name, script_args=script_args)
        context = MagicMock()
        result = op.execute(context)

        expected_command = ["python", "myscript.py", "--foo", "bar", "--num", "42"]
        container_overrides = op.overrides["containerOverrides"][0]
        assert container_overrides["command"] == expected_command

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)

        assert "script_args" in PythonScriptOperator.template_fields
        assert "script_name" in PythonScriptOperator.template_fields


def test_python_script_operator_appends_to_user_container_overrides():
    from unittest.mock import MagicMock, patch

    from dags.lib.operators.ecs import PythonScriptOperator

    user_overrides = {"containerOverrides": [{"name": "user-container", "command": ["echo", "hi"]}]}
    script_name = "myscript.py"
    script_args = {"foo": "bar"}

    with patch("dags.lib.operators.ecs.ecs.EcsRunTaskOperator.execute", return_value="done") as mock_super_execute:
        op = PythonScriptOperator(
            task_id="test_task",
            script_name=script_name,
            script_args=script_args,
            overrides=user_overrides,
        )
        context = MagicMock()
        result = op.execute(context)

        # The operator should append its own override to the list
        container_overrides = op.overrides["containerOverrides"]
        assert len(container_overrides) == 2
        assert container_overrides[0] == {"name": "user-container", "command": ["echo", "hi"]}
        assert container_overrides[1] == {
            "name": config.ecs_container_name,
            "command": ["python", "myscript.py", "--foo", "bar"],
        }

        assert result == "done"
        mock_super_execute.assert_called_once_with(context)
