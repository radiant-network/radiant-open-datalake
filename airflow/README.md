# radiant-open-datalake airflow

This directory contains Airflow DAGs and related code for orchestrating workflows associated to public datasets.


## Developpers

### Run unit tests

1) Create a virtual environment with dependencies installed.
```sh
python -m venv .venv
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/.airflow_home
pip install --upgrade pip
make install-dev
```

Note: we strongly to use python 3.12 as will be used in the AWS environment.

2) Run the tests:
```sh
make test
```