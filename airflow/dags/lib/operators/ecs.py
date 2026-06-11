import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from functools import lru_cache

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators import ecs

from dags.lib import config


def _parse_csv(env_val):
    return tuple(v.strip() for v in env_val.split(",") if v.strip())


_REQUIRED_ENV_VARS = {
    "cluster": "OPENDATALAKE_ECS_CLUSTER",
    "subnets": "OPENDATALAKE_ECS_SUBNETS",
    "security_groups": "OPENDATALAKE_ECS_SECURITY_GROUPS",
    "task_definition": "OPENDATALAKE_TASK_OPERATOR_TASK_DEFINITION",
}


@dataclass(frozen=True)
class EcsConfig:
    cluster: str
    subnets: tuple[str, ...]
    security_groups: tuple[str, ...]
    container_name: str
    task_definition: str | None
    awslogs_group: str | None
    awslogs_region: str | None
    awslogs_stream_prefix: str | None

    @classmethod
    @lru_cache(maxsize=1)
    def from_env(cls) -> "EcsConfig":
        return cls(
            cluster=os.getenv(_REQUIRED_ENV_VARS["cluster"], ""),
            subnets=_parse_csv(os.getenv(_REQUIRED_ENV_VARS["subnets"], "")),
            security_groups=_parse_csv(os.getenv(_REQUIRED_ENV_VARS["security_groups"], "")),
            container_name=f"opendatalake-operator-{config.environment}-etl-container",
            task_definition=os.getenv(_REQUIRED_ENV_VARS["task_definition"]),
            awslogs_group=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_GROUP"),
            awslogs_region=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_REGION"),
            awslogs_stream_prefix=os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_PREFIX"),
        )

    def missing_required(self) -> dict[str, str]:
        return {field: env_var for field, env_var in _REQUIRED_ENV_VARS.items() if not getattr(self, field)}


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

        missing = ecs_config.missing_required()
        if missing:
            raise AirflowException(
                f"Incomplete ECS configuration; missing field(s): {', '.join(missing)} "
                f"(when using environment-based config, set: {', '.join(missing.values())})"
            )

        kwargs.setdefault("overrides", {})
        if "containerOverrides" in kwargs["overrides"]:
            logging.warning(
                "A user-provided containerOverrides was detected. "
                "The operator will append its own container override. "
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
                "subnets": list(ecs_config.subnets),
                "assignPublicIp": "DISABLED",
                "securityGroups": list(ecs_config.security_groups),
            }
        },
        aws_conn_id="aws_default",
    )
