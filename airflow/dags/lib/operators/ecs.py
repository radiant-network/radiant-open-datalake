import logging
from datetime import timedelta

from airflow.providers.amazon.aws.operators import ecs

from dags.lib import config
from dags.lib.config import ECSEnv


class PythonScriptOperator(ecs.EcsRunTaskOperator):
    template_fields = (*ecs.EcsRunTaskOperator.template_fields, "script_args", "script_name")

    def __init__(
        self,
        script_name: str,
        script_args: dict,
        container_name: str = config.ecs_container_name,
        ecs_env: ECSEnv = config.ecs_env,
        **kwargs,
    ):
        kwargs.setdefault("overrides", {})
        if "containerOverrides" in kwargs["overrides"]:
            logging.warning(
                "A user-provided containerOverrides was detected."
                "The operator will append its own container override."
                "Ensure there are no duplicate or conflicting definitions."
            )

        super().__init__(**_get_ecs_context(ecs_env), **kwargs)
        self.script_name = script_name
        self.script_args = script_args
        self.container_name = container_name

    def execute(self, context, **kwargs):
        command = ["python", self.script_name]
        for k, v in self.script_args.items():
            command.append(f"--{k}")
            command.append(str(v))

        self.overrides = self.overrides or {}
        self.overrides.setdefault("containerOverrides", []).append({"name": self.container_name, "command": command})

        return super().execute(context, **kwargs)


def _get_ecs_context(ecs_env: ECSEnv):
    return dict(
        cluster=ecs_env.ECS_CLUSTER,
        launch_type="FARGATE",
        task_definition=config.ecs_task_definition,
        awslogs_group=config.awslogs_group,
        awslogs_region=config.awslogs_region,
        awslogs_stream_prefix=config.awslogs_stream_prefix,
        awslogs_fetch_interval=timedelta(seconds=5),
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ecs_env.ECS_SUBNETS,
                "assignPublicIp": "DISABLED",
                "securityGroups": ecs_env.ECS_SECURITY_GROUPS,
            }
        },
        aws_conn_id="aws_default",
    )
