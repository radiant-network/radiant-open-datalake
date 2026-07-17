# radiant-open-datalake airflow

This directory contains Airflow DAGs and related code for orchestrating workflows associated to public datasets.


## Runtime Requirements

### ECS Operator

- All required Airflow variables and environment variables must be set. See [opendatalake.lib.config](opendatalake/lib/config.py) for details.

- The ECS container must be pre-configured so that S3 credentials and connection info are available at runtime (e.g., via environment variables or IAM roles) to allow the S3 hook to function properly.


## Operations

Manual deployment (no CI automation yet). 

### Deploy DAGs + library to MWAA

From `airflow/`:

(Make sure your AWS credentials for CHOP's Cloud are loaded into your environment.)
```sh
# Login into CHOP's Cloud, example with SSO login
aws sso login --{Your Radiant-TST Profile}

# MWAA DAGs bucket — confirm name with infra
aws s3 sync opendatalake s3://radiant-tst-airflow-qa/dags/opendatalake --exclude "__pycache__/*" --exclude "*.pyc"
```

- [ ] Target is `dags/opendatalake/` — required so `from opendatalake.lib ...`
      imports resolve. Same layout as the sandbox mount.
- [ ] `opendatalake/lib/` ships in the same sync — no separate step.
- [ ] MWAA picks up changes in ~30s, no restart.

### Deploy Spark JAR (for the EMR operator)

From repo root:

```sh
(cd spark && sbt assembly)   # -> spark/target/scala-2.12/radiant-open-datalake-spark.jar

aws s3 cp spark/target/scala-2.12/radiant-open-datalake-spark.jar \
  s3://<bucket>/<prefix>/   # path must equal OPENDATALAKE_EMR_JAR_S3_PATH
```

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
