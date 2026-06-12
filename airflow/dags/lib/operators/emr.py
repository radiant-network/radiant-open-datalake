import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.utils.context import Context

from dags.lib import config

DEFAULT_ENTRY_CLASS = "org.radiant.opendatalake.ImportPublicTable"
SPARK_CONF_CATALOG_NAME = "opendatalake"

# These should never be set directly when using the Airflow operator. Those are infrastructure
# specific variables and should be inferred from elsewhere. Manually setting those means you are
# doing something that is not intended.
_RESERVED_KWARGS = ("application_id", "execution_role_arn", "job_driver", "region_name")


# The following environment variables need to be available in Airflow's environment.
# Instantiating the class will fail if any of those are missing.
_REQUIRED_ENV_VARS = {
    "application_id": "OPENDATALAKE_EMR_APPLICATION_ID",
    "execution_role_arn": "OPENDATALAKE_EMR_EXECUTION_ROLE_ARN",
    "jar_s3_path": "OPENDATALAKE_EMR_JAR_S3_PATH",
    "warehouse_s3": "OPENDATALAKE_EMR_WAREHOUSE_S3",
    "glue_catalog_id": "OPENDATALAKE_EMR_GLUE_CATALOG_ID",
    "region": "OPENDATALAKE_EMR_REGION",
    "glue_database": "OPENDATALAKE_EMR_GLUE_DATABASE",
    "cloudwatch_log_group": "OPENDATALAKE_EMR_LOG_GROUP",
}


@dataclass(frozen=True)
class EmrServerlessConfig:
    application_id: str
    execution_role_arn: str
    jar_s3_path: str
    warehouse_s3: str
    glue_catalog_id: str
    region: str
    glue_database: str
    cloudwatch_log_group: str
    cloudwatch_log_stream_prefix: str | None
    cloudwatch_region: str | None

    @classmethod
    @lru_cache(maxsize=1)
    def from_env(cls) -> "EmrServerlessConfig":
        return cls(
            application_id=os.getenv(_REQUIRED_ENV_VARS["application_id"], ""),
            execution_role_arn=os.getenv(_REQUIRED_ENV_VARS["execution_role_arn"], ""),
            jar_s3_path=os.getenv(_REQUIRED_ENV_VARS["jar_s3_path"], ""),
            warehouse_s3=os.getenv(_REQUIRED_ENV_VARS["warehouse_s3"], ""),
            glue_catalog_id=os.getenv(_REQUIRED_ENV_VARS["glue_catalog_id"], ""),
            region=os.getenv(_REQUIRED_ENV_VARS["region"], ""),
            glue_database=os.getenv(_REQUIRED_ENV_VARS["glue_database"], ""),
            cloudwatch_log_group=os.getenv(_REQUIRED_ENV_VARS["cloudwatch_log_group"], ""),
            cloudwatch_log_stream_prefix=os.getenv("OPENDATALAKE_EMR_LOG_PREFIX"),
            cloudwatch_region=os.getenv("OPENDATALAKE_EMR_LOG_REGION"),
        )

    def missing_required(self) -> dict[str, str]:
        return {field: env_var for field, env_var in _REQUIRED_ENV_VARS.items() if not getattr(self, field)}


class EmrServerlessJobOperator(EmrServerlessStartJobOperator):
    """Launch the opendatalake Spark ETL on EMR Serverless.

    Single entrypoint for EMR Serverless jobs: DAG authors pass only the dataset arguments
    (and optional Spark tuning); everything else is derived from config.

    Configuration:
        Infra config comes from a colocated `EmrServerlessConfig`.
        It is used to build the Iceberg/Glue Spark configuration and the spark-submit `job_driver`.

    Spark conf overrides:
        You can override any base configuration (overloading the `key` in the configuration mapping).
        Use at your own risk.

    Driver logs:
        Forwarded into the Airflow task log by default. A CloudWatch monitoring config is injected
        (unless the caller already set one), and SPARK_DRIVER stdout/stderr are streamed into the
        task log once the job reaches a terminal state — even on failure.
    """

    template_fields = (
        *EmrServerlessStartJobOperator.template_fields,
        "cloudwatch_log_group",
        "cloudwatch_log_stream_prefix",
    )

    def __init__(
        self,
        *,
        entry_point_arguments: list[str],
        entry_class: str = DEFAULT_ENTRY_CLASS,
        spark_conf: dict | None = None,
        name: str | None = None,
        emr_config: EmrServerlessConfig | None = None,
        deferrable: bool = True,  # Deferrable by default
        waiter_delay: int = 60,
        waiter_max_attempts: int = 480,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        emr_config = emr_config or EmrServerlessConfig.from_env()

        missing = emr_config.missing_required()
        if missing:
            raise AirflowException(
                f"Incomplete EMR Serverless configuration; missing field(s): {', '.join(missing)} "
                f"(when using environment-based config, set: {', '.join(missing.values())})"
            )

        # When deferred, we need to wait for completion otherwise we lose logs.
        if deferrable and not wait_for_completion:
            raise AirflowException("EmrServerlessJobOperator requires wait_for_completion=True when deferrable=True.")

        reserved = [k for k in _RESERVED_KWARGS if k in kwargs]
        if reserved:
            raise AirflowException(
                f"{', '.join(reserved)} are managed by EmrServerlessJobOperator (from EmrServerlessConfig) "
                "and must not be passed directly."
            )

        kwargs["deferrable"] = deferrable
        kwargs["waiter_delay"] = waiter_delay
        kwargs["waiter_max_attempts"] = waiter_max_attempts
        kwargs["wait_for_completion"] = wait_for_completion

        cloudwatch_log_group = emr_config.cloudwatch_log_group
        cloudwatch_log_stream_prefix = emr_config.cloudwatch_log_stream_prefix
        cloudwatch_region = emr_config.cloudwatch_region or emr_config.region

        merged_conf = {**_base_spark_conf(emr_config), **(spark_conf or {})}
        job_driver = _build_job_driver(emr_config.jar_s3_path, entry_class, entry_point_arguments, merged_conf)

        kwargs["configuration_overrides"] = self._merge_monitoring(
            kwargs.get("configuration_overrides"),
            cloudwatch_log_group,
            cloudwatch_log_stream_prefix,
        )

        super().__init__(
            application_id=emr_config.application_id,
            execution_role_arn=emr_config.execution_role_arn,
            job_driver=job_driver,
            name=name or f"opendatalake-{config.environment}-{{{{ ts_nodash }}}}",
            region_name=emr_config.region,
            **kwargs,
        )
        self.cloudwatch_log_group = cloudwatch_log_group
        self.cloudwatch_log_stream_prefix = cloudwatch_log_stream_prefix
        self.cloudwatch_region = cloudwatch_region

    @staticmethod
    def _merge_monitoring(overrides: dict | None, log_group: str, stream_prefix: str | None) -> dict:
        overrides = dict(overrides or {})
        monitoring = dict(overrides.get("monitoringConfiguration") or {})
        cw = dict(monitoring.get("cloudWatchLoggingConfiguration") or {})
        cw.setdefault("enabled", True)
        cw.setdefault("logGroupName", log_group)
        if stream_prefix and "logStreamNamePrefix" not in cw:
            cw["logStreamNamePrefix"] = stream_prefix
        monitoring["cloudWatchLoggingConfiguration"] = cw
        overrides["monitoringConfiguration"] = monitoring
        return overrides

    def execute(self, context: Context, event: dict | None = None) -> Any:
        if self.deferrable:
            return super().execute(context, event)
        try:
            return super().execute(context)
        finally:
            self._forward_driver_logs()

    def execute_complete(self, context: Context, event: dict | None = None) -> Any:
        try:
            return super().execute_complete(context, event)
        finally:
            # self.job_id is not restored across deferral; read it from the job-completion event.
            self._forward_driver_logs(self._event_job_run_id(event))

    @staticmethod
    def _event_job_run_id(event: dict | None) -> str | None:
        return ((event or {}).get("job_details") or {}).get("job_id")

    def _forward_driver_logs(self, job_run_id: str | None = None) -> None:
        log = logging.getLogger("airflow.task")
        # Try/Catch to avoid failing the Airflow task because of CloudWatch errors.
        try:
            job_run_id = job_run_id or getattr(self, "job_id", None)
            if not job_run_id:
                log.warning("No job_run_id available; skipping driver log forwarding.")
                return

            hook = AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.cloudwatch_region)
            base = f"applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER"
            if self.cloudwatch_log_stream_prefix:
                base = f"{self.cloudwatch_log_stream_prefix}/{base}"
            not_found = hook.get_conn().exceptions.ResourceNotFoundException

            for kind in ["stdout", "stderr"]:
                stream = f"{base}/{kind}"
                log.info("===== SPARK_DRIVER/%s (%s) =====", kind, stream)
                try:
                    empty = True
                    for log_event in hook.get_log_events(log_group=self.cloudwatch_log_group, log_stream_name=stream):
                        log.info(log_event["message"])
                        empty = False
                    if empty:
                        log.info("(no events)")
                except not_found:
                    log.warning("Stream not found: %s", stream)
        except Exception:
            log.warning("Failed to forward EMR driver logs; continuing.", exc_info=True)


def _base_spark_conf(cfg: EmrServerlessConfig) -> dict[str, str]:
    return {
        "spark.dynamicAllocation.maxExecutors": "4",
        "spark.dynamicAllocation.initialExecutors": "1",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.client.factory.class": (
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        ),
        "spark.hadoop.hive.metastore.glue.catalogid": cfg.glue_catalog_id,
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.default-namespace": cfg.glue_database,
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.glue.id": cfg.glue_catalog_id,
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.warehouse": cfg.warehouse_s3,
        f"spark.sql.catalog.{SPARK_CONF_CATALOG_NAME}.client.region": cfg.region,
        "spark.sql.defaultCatalog": SPARK_CONF_CATALOG_NAME,
        "spark.sql.shuffle.partitions": "16",
    }


def _build_job_driver(jar_s3_path: str, entry_class: str, args: list[str], conf: dict[str, str]) -> dict:
    # Add quotes "" to conf values with whitespaces because spark-submit splits its params on whitespace.
    parts = [f"--class {entry_class}"]
    for k, v in conf.items():
        token = f"{k}={v}"
        parts.append(f'--conf "{token}"' if " " in token else f"--conf {token}")
    params = " ".join(parts)
    return {
        "sparkSubmit": {
            "entryPoint": jar_s3_path,
            "entryPointArguments": args,
            "sparkSubmitParameters": params,
        }
    }
