"""Sandbox-only drop-in replacement for the EMR Serverless import operator.

Runs the Open Datalake Spark ETL as `spark-submit --master local[*]` inside a
KubernetesPodOperator, reading raw files from the in-cluster MinIO (via Hadoop S3A) and writing
Iceberg tables through the in-cluster Apache Polaris REST catalog (credential vending).

Keeps the class name `EmrServerlessJobOperator` so switching the DAG only rewrites the import path
(`operators.emr` -> `operators.spark_k8s`). EMR-only kwargs (waiter_*, deferrable, ...) are accepted
and ignored. Do NOT commit the swap: this operator has no place in the AWS deployment.
"""

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from opendatalake.lib.operators.emr import job_name_timestamp  # noqa: F401

DEFAULT_ENTRY_CLASS = "org.radiant.opendatalake.ImportPublicTable"
SPARK_IMAGE = "ghcr.io/radiant-network/opendatalake-spark:latest"
JAR_PATH = "/opt/app/radiant-open-datalake-spark.jar"

# Extra classpath dir baked into the image; holds config/dev.conf (see Dockerfile.opendatalake.spark).
_CONF_EXTRA_CLASSPATH = "/opt/app/conf-extra"

# Spark conf for the sandbox: raw reads via S3A against MinIO, Iceberg writes via Polaris.
_LOCAL_SPARK_CONF = {
    # --- raw input: Hadoop S3A -> MinIO ---
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.endpoint": "http://opendatalake-minio:9000",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    # --- Iceberg output: Polaris REST catalog (metadata) + direct MinIO S3 access (data) ---
    "spark.sql.catalog.opendatalake.type": "rest",
    "spark.sql.catalog.opendatalake.uri": "http://polaris:8181/api/catalog",
    "spark.sql.catalog.opendatalake.warehouse": "opendatalake",
    "spark.sql.catalog.opendatalake.credential": "root:s3cr3t",
    "spark.sql.catalog.opendatalake.scope": "PRINCIPAL_ROLE:ALL",
    "spark.sql.catalog.opendatalake.token-refresh-enabled": "false",
    "spark.sql.catalog.opendatalake.client.region": "us-east-1",
    # S3FileIO must talk to MinIO, not AWS. Set the endpoint + static creds explicitly rather than
    # relying on Polaris credential vending (vended endpoint wasn't applied -> S3FileIO fell back to
    # real AWS S3 and got 404 NoSuchBucket). Polaris here manages metadata only.
    "spark.sql.catalog.opendatalake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.opendatalake.s3.endpoint": "http://opendatalake-minio:9000",
    "spark.sql.catalog.opendatalake.s3.path-style-access": "true",
    "spark.sql.catalog.opendatalake.s3.access-key-id": "admin",
    "spark.sql.catalog.opendatalake.s3.secret-access-key": "password",
    # dev.conf ships only inside the fat JAR as config/prd.conf; expose config/dev.conf here.
    "spark.driver.extraClassPath": _CONF_EXTRA_CLASSPATH,
}

_CONTAINER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1", "memory": "4Gi"},
    limits={"cpu": "2", "memory": "6Gi"},
)


def _conf_flags(spark_conf: dict[str, str]) -> list[str]:
    flags: list[str] = []
    for key, value in spark_conf.items():
        flags += ["--conf", f"{key}={value}"]
    return flags


class EmrServerlessJobOperator(KubernetesPodOperator):
    def __init__(
        self,
        *,
        entry_point_arguments: list,
        entry_class: str = DEFAULT_ENTRY_CLASS,
        spark_conf: dict | None = None,
        name: str | None = None,  # EMR job name; unused (pod name derives from task_id)
        driver_memory: str = "4g",
        **kwargs,
    ):
        # Drop EMR Serverless-only kwargs so they never reach KubernetesPodOperator.
        for emr_only in (
            "waiter_delay",
            "waiter_max_attempts",
            "wait_for_completion",
            "deferrable",
            "emr_config",
        ):
            kwargs.pop(emr_only, None)

        merged_conf = {**_LOCAL_SPARK_CONF, **(spark_conf or {})}
        # spark-submit options first, then the app JAR, then the ETL command/args (entry_point_arguments,
        # which may contain templated XComArgs — `arguments` is a KubernetesPodOperator template field).
        arguments = [
            *_conf_flags(merged_conf),
            "--master",
            "local[*]",
            "--driver-memory",
            driver_memory,
            "--class",
            entry_class,
            JAR_PATH,
            *entry_point_arguments,
        ]

        super().__init__(
            namespace="opendatalake",
            image=SPARK_IMAGE,
            image_pull_policy="IfNotPresent",
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=arguments,
            container_resources=_CONTAINER_RESOURCES,
            get_logs=True,
            is_delete_operator_pod=True,
            **kwargs,
        )
