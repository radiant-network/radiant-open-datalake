import pathlib
from unittest.mock import MagicMock

import pytest
from airflow.models import DagBag


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
