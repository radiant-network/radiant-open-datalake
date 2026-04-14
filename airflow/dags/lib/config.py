"""
Holds configuration values and global constants for the project.

Note: Some Airflow or environment variables may be missing. This is intentional,
as certain operators only require a subset of the configuration and do not need
all variables to be present.
"""

import os
from dataclasses import dataclass

# Importing from airflow.models instead of airflow.sdk to avoid test initialization issues.
# This is fixed in newer Airflow versions, so you can switch back to airflow.sdk if we upgrade.
from airflow.models import Variable


def _parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]


_MISSING_VARIABLE = "MISSING"

# S3 settings
environment = Variable.get("environment", "dev")
s3_conn_id = "opendatalake_s3"
raw_datalake_bucket = f"opendatalake-{environment}"

# DAGs settings

DAG_ID_PREFIX = "opendatalake"
DAG_DEFAULT_TAGS = [DAG_ID_PREFIX]
DAG_DISPLAY_NAME_PREFIX = "Open Datalake"

# Assets settings
ASSETS_URI_PREFIX = "opendatalake"


# Pools
DOWNLOAD_TASKS_POOL = "opendatalake_download_tasks_pool"

# ------------- #
# ECS Settings  #
# ------------- #

ecs_container_name = f"opendatalake-operator-{environment}-etl-container"


@dataclass(frozen=True)
class ECSEnv:
    ECS_CLUSTER: str | None = None
    ECS_SUBNETS: list[str] | None = None
    ECS_SECURITY_GROUPS: list[str] | None = None
    ECS_S3_WORKSPACE: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "ECS_CLUSTER", Variable.get("aws_ecs_cluster", ""))
        object.__setattr__(self, "ECS_SUBNETS", _parse_list(Variable.get("aws_ecs_subnets", "")))
        object.__setattr__(self, "ECS_SECURITY_GROUPS", _parse_list(Variable.get("aws_ecs_security_groups", "")))
        object.__setattr__(self, "ECS_S3_WORKSPACE", Variable.get("aws_ecs_s3_workspace", ""))


ecs_env = ECSEnv()


# Using environment variables for these settings because they are injected via the
# terraform mwaa startup script and not stored in airflow variables.
ecs_task_definition = os.getenv("OPENDATALAKE_TASK_OPERATOR_TASK_DEFINITION")
awslogs_group = os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_GROUP")
awslogs_region = os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_REGION")
awslogs_stream_prefix = os.getenv("OPENDATALAKE_TASK_OPERATOR_LOG_PREFIX")
