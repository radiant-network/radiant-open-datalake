# radiant-open-datalake airflow

This directory contains Airflow DAGs and related code for orchestrating workflows associated to public datasets.


## Runtime Requirements

### ECS Operator

- All required Airflow variables and environment variables must be set. See [dags.lib.config](dags/lib/config.py) for details.

- The ECS container must be pre-configured so that S3 credentials and connection info are available at runtime (e.g., via environment variables or IAM roles) to allow the S3 hook to function properly.


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

### Sandbox for local integration testing

A local sandbox is available to run DAGs locally with Airflow.

AWS-dependent operators are not supported, but this can still be useful for testing some DAGs.

See [sandbox/README.md](sandbox/README.md) for instructions.
