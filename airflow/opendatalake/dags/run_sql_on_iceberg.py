"""Ad-hoc SQL runner over the Open Datalake Iceberg catalog.

Manual DAG: takes a SQL query as a DAG param and runs it on EMR Serverless (Spark). The job runs
under the EMR Serverless execution role, which is the role granted access to the Glue-backed Iceberg
tables — that is why the query must run there and not inside the Airflow worker.

Self-contained: a tiny PySpark script is uploaded to S3 at run time and launched as the job's entry
point (no fat-JAR rebuild). The result is printed with `DataFrame.show(...)` to the driver stdout,
which `EmrServerlessJobOperator` forwards into the `run_sql` task log (on success and on failure).
Read-only by intent — results are logged, nothing is written back.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from airflow.sdk.definitions.param import Param

from opendatalake.lib import config
from opendatalake.lib.operators.emr import EmrServerlessJobOperator, job_name_timestamp

# The PySpark entry point, uploaded to S3 verbatim before the job launches. Kept inline so the DAG is
# the only thing to deploy. Content is fixed, so a single overwritten key is safe across runs.
_SCRIPT_KEY = "scripts/run_sql/run_sql.py"

_RUN_SQL_SCRIPT = '''\
"""Run one SQL query against the Iceberg catalog and print the result (EMR Serverless PySpark job)."""
import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--num-rows", type=int, default=20)
    parser.add_argument("--truncate", type=int, default=0)  # 0 -> no truncation
    args = parser.parse_args()

    spark = SparkSession.builder.appName("opendatalake-run-sql").getOrCreate()
    print("===== opendatalake-run-sql =====")
    print("Query: " + args.query)
    df = spark.sql(args.query)
    df.show(n=args.num_rows, truncate=(args.truncate if args.truncate > 0 else False))
    spark.stop()


if __name__ == "__main__":
    main()
'''


def _script_s3_uri() -> str:
    return f"s3://{config.raw_datalake_bucket}/{_SCRIPT_KEY}"


def _params() -> dict:
    return {
        "query": Param(
            None,
            type=["null", "string"],
            title="SQL query",
            description=(
                "SQL to run on the Iceberg catalog. Reference tables as "
                "`reference.<table>`; contract tables need a branch, e.g. "
                "``reference.clinvar_v1.`branch_20260715` ``."
            ),
        ),
        "num_rows": Param(
            20,
            type="integer",
            minimum=1,
            maximum=1000,
            title="Rows to display",
        ),
        "truncate": Param(
            0,
            type="integer",
            minimum=0,
            title="Column truncation width (0 = no truncation)",
        ),
    }


@dag(
    dag_id=f"{config.DAG_ID_PREFIX}-run-sql",
    dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Run SQL on Iceberg",
    schedule=None,
    params=_params(),
    tags=config.DAG_DEFAULT_TAGS + [f"{config.DAG_ID_PREFIX}_{t}" for t in ["sql", "manual", "utility"]],
    catchup=False,
)
def run_sql_on_iceberg():
    @task(task_display_name="[PyOp] Prepare SQL job")
    def prepare_job(params=None) -> dict:
        params = params or {}
        query = params.get("query")
        if not query or not str(query).strip():
            raise ValueError("The 'query' param is required; supply a SQL statement to run.")
        # Upload the entry-point script (idempotent: fixed content, fixed key).
        S3Hook(config.s3_conn_id).load_string(
            _RUN_SQL_SCRIPT,
            key=_SCRIPT_KEY,
            bucket_name=config.raw_datalake_bucket,
            replace=True,
        )
        # entryPointArguments must be strings; stringify here so the templated job driver stays valid.
        return {
            "query": query,
            "num_rows": str(int(params.get("num_rows") or 20)),
            "truncate": str(int(params.get("truncate") or 0)),
        }

    prepared = prepare_job()

    # PySpark mode: entry_point is the uploaded script; the operator adds the fat JAR as spark.jars for
    # the Iceberg/Glue classes and forwards the driver logs (query result) into this task's log.
    EmrServerlessJobOperator(
        task_id="run_sql",
        task_display_name="[EMR] Run SQL on Iceberg",
        entry_point=_script_s3_uri(),
        entry_point_arguments=[
            "--query",
            prepared["query"],
            "--num-rows",
            prepared["num_rows"],
            "--truncate",
            prepared["truncate"],
        ],
        # Small job: cap executors and shuffle partitions well below the import defaults. Pin
        # executor.instances/initialExecutors to 1 — EMR Serverless defaults executor.instances to 3,
        # which would exceed maxExecutors=2 and fail job validation.
        spark_conf={
            "spark.dynamicAllocation.maxExecutors": "2",
            "spark.dynamicAllocation.initialExecutors": "1",
            "spark.executor.instances": "1",
            "spark.sql.shuffle.partitions": "8",
        },
        name=f"opendatalake-{config.environment}-sql-{job_name_timestamp()}",
        waiter_delay=30,
        waiter_max_attempts=120,
    )


run_sql_on_iceberg()
