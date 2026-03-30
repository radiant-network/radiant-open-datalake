# radiant-open-datalake airflow

This directory contains Airflow DAGs and related code for orchestrating workflows associated to public datasets.


## Developpers

### Run unit tests

1) Create a virtual environment with dependencies installed:
```sh
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
make install-dev
```

2) Setup airflow db:
```sh
make airflow-reset
```

3) Run the tests:
```sh
make test
```