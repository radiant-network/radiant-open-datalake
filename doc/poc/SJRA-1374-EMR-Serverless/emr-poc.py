from airflow import DAG

from radiant.dags.operators.emr import EmrServerlessStartJobWithLogsOperator

APPLICATION_ID = "00g59743sbl50409"
EXECUTION_ROLE_ARN = "arn:aws:iam::418295705741:role/service-role/AmazonEMR-ExecutionRole-1777389601404"

JAR_S3 = "s3://radiant-tst-datalake-qa/opendatalake/jars/radiant-open-datalake-spark.jar"
WAREHOUSE_S3 = "s3://radiant-tst-datalake-qa/opendatalake/"
GLUE_CATALOG_ID = "418295705741"
AWS_REGION = "us-east-1"

LOG_GROUP_NAME = "/aws/emr-serverless/poc-emr-opendatalake"
LOG_STREAM_PREFIX = "poc_emr"

default_args = {
    "owner": "poc-emr",
}

version = "1.0.0"

ENTRY_CLASS = "org.radiant.opendatalake.ImportPublicTable"
ENTRY_ARGS = [
    "clinvar",
    "--config", "config/poc.conf",
    "--steps", "default",
    "--app-name", "clinvar-poc",
]

SPARK_CONF = {
    # Setup upper boundary
    "spark.dynamicAllocation.maxExecutors": "4",
    "spark.dynamicAllocation.initialExecutors": "1",

    # Iceberg + Glue catalog
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalogImplementation": "in-memory",
    "spark.sql.catalog.opendatalake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.opendatalake.default-namespace": "opendatalake_poc",
    "spark.sql.catalog.opendatalake.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.opendatalake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.opendatalake.glue.id": GLUE_CATALOG_ID,
    "spark.sql.catalog.opendatalake.warehouse": WAREHOUSE_S3,
    "spark.sql.catalog.opendatalake.client.region": AWS_REGION,
    "spark.sql.defaultCatalog": "opendatalake",

    # Small dataset shuffle
    "spark.sql.shuffle.partitions": "16",
}

SPARK_SUBMIT_PARAMS = " ".join(
    [f"--class {ENTRY_CLASS}"]
    + [f"--conf {k}={v}" for k, v in SPARK_CONF.items()]
)

JOB_DRIVER = {
    "sparkSubmit": {
        "entryPoint": JAR_S3,
        "entryPointArguments": ENTRY_ARGS,
        "sparkSubmitParameters": SPARK_SUBMIT_PARAMS,
    }
}

with DAG(
    dag_id="x-emr-serverless-poc",
    dag_display_name="[POC] EMR OpenDataLake POC",
    default_args=default_args,
    description="EMR Serverless PoC for OpenDataLake — clinvar import",
    catchup=False,
    tags=["emr", "serverless", "opendatalake", "poc", "manual"],
) as dag:
    run_emr_job = EmrServerlessStartJobWithLogsOperator(
        task_id="start_clinvar_job",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        name=f"poc_emr_clinvar_{version}_" + "{{ ts_nodash }}",
        job_driver=JOB_DRIVER,
        cloudwatch_log_group=LOG_GROUP_NAME,
        cloudwatch_log_stream_prefix=LOG_STREAM_PREFIX,
        cloudwatch_region=AWS_REGION,
        enable_application_ui_links=True,
        waiter_delay=30,
        waiter_max_attempts=60,
        pipe_stderr=True,
    )
