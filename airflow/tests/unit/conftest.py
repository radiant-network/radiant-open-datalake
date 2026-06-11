import os
import pathlib
from unittest.mock import MagicMock

import pytest
from airflow.models import DagBag

from dags.lib.operators.ecs import _REQUIRED_ENV_VARS

for _env_var in _REQUIRED_ENV_VARS.values():
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
    return DagBag(dag_folder=pathlib.Path("dags"), include_examples=False)
