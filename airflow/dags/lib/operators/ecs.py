import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from functools import lru_cache

from airflow.providers.amazon.aws.operators import ecs

from dags.lib import config


def _parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]


@dataclass(frozen=True)
class EcsConfig:
    cluster: str
    subnets: list[str]
    security_groups: list[str]
    s3_workspace: str
    container_name: str
    task_definition: str | None
    awslogs_group: str | None
    awslogs_region: str | None
    awslogs_stream_prefix: str | None

    @classmethod
    @lru_cache(maxsize=1)
    def from_env(cls) -> "EcsConfig":
        # All values are infra facts injected as environment variables by the
        # terraform mwaa startup script.
        return cls(
            cluster=os.getenv("OPENDATALAKE_ECS_CLUSTER", ""),
            subnets=_parse_list(os.getenv("OPENDATALAKE_ECS_SUBNETS", "")),
            security_groups=_parse_list(os.getenv("OPENDATALAKE_ECS_SECURITY_GROUPS", "")),
            s3_workspace=os.getenv("OPENDATALAKE_ECS_S3_WORKSPACE", ""),
            container_name=f"opendatalake-operator-{config.environment}-etl-container",
            task_definition=os.getenv("OPENDATALAKE_TASK_OPERATOR_TASK_DEFINITION"),
            awslogs_group=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_GROUP"),
            awslogs_region=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_REGION"),
            awslogs_stream_prefix=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_PREFIX"),
        )


class PythonScriptOperator(ecs.EcsRunTaskOperator):
    template_fields = (*ecs.EcsRunTaskOperator.template_fields, "script_args", "script_name")

    def __init__(
        self,
        script_name: str,
        script_args: dict,
        container_name: str | None = None,
        ecs_config: EcsConfig | None = None,
        **kwargs,
    ):
        ecs_config = ecs_config or EcsConfig.from_env()

        kwargs.setdefault("overrides", {})
        if "containerOverrides" in kwargs["overrides"]:
            logging.warning(
                "A user-provided containerOverrides was detected."
                "The operator will append its own container override."
                "Ensure there are no duplicate or conflicting definitions."
            )

        super().__init__(**_get_ecs_context(ecs_config), **kwargs)
        self.script_name = script_name
        self.script_args = script_args
        self.container_name = container_name or ecs_config.container_name

    def execute(self, context, **kwargs):
        command = ["python", self.script_name]
        for k, v in self.script_args.items():
            command.append(f"--{k}")
            command.append(str(v))

        self.overrides = self.overrides or {}
        self.overrides.setdefault("containerOverrides", []).append({"name": self.container_name, "command": command})

        return super().execute(context, **kwargs)


def _get_ecs_context(ecs_config: EcsConfig):
    return dict(
        cluster=ecs_config.cluster,
        launch_type="FARGATE",
        task_definition=ecs_config.task_definition,
        awslogs_group=ecs_config.awslogs_group,
        awslogs_region=ecs_config.awslogs_region,
        awslogs_stream_prefix=ecs_config.awslogs_stream_prefix,
        awslogs_fetch_interval=timedelta(seconds=5),
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ecs_config.subnets,
                "assignPublicIp": "DISABLED",
                "securityGroups": ecs_config.security_groups,
            }
        },
        aws_conn_id="aws_default",
    )
