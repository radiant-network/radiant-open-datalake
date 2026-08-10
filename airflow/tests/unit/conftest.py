import os
import pathlib
from unittest.mock import MagicMock

import pytest
from airflow.models import DagBag

from opendatalake.lib.domain.source_configs.spliceai import ACCESS_TOKEN_ENV_VAR as _SPLICEAI_TOKEN_ENV_VAR
from opendatalake.lib.domain.source_configs.topmed import COOKIE_ENV_VAR as _TOPMED_COOKIE_ENV_VAR
from opendatalake.lib.operators.ecs import _REQUIRED_ENV_VARS as _ECS_REQUIRED_ENV_VARS
from opendatalake.lib.operators.emr import _REQUIRED_ENV_VARS as _EMR_REQUIRED_ENV_VARS

for _env_var in (
    *_ECS_REQUIRED_ENV_VARS.values(),
    *_EMR_REQUIRED_ENV_VARS.values(),
    _SPLICEAI_TOKEN_ENV_VAR,
    _TOPMED_COOKIE_ENV_VAR,
):
    os.environ.setdefault(_env_var, f"test-{_env_var.lower()}")


@pytest.fixture
def s3_hook():
    return MagicMock()


@pytest.fixture
def s3_client(s3_hook):
    s3_client = MagicMock()
    s3_hook.get_conn.return_value = s3_client
    return s3_client


@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=pathlib.Path("opendatalake/dags"), include_examples=False)
