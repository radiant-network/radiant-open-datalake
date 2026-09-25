"""Backfill the `latest` tag on Iceberg tables published before tagging existed.

Manual DAG. `WapLoader` moves a `latest` tag to every publish since SJRA-1889, but tables published
before that have version branches and no tag, so a consumer reading `VERSION AS OF 'latest'` finds
nothing. This job inspects each table, picks the version branch whose snapshot is the most recently
committed one holding rows, and points `latest` at it.

`main` and `audit_*` are never candidates: `main` is the deliberately empty clean base and `audit_*`
is the transient staging branch, neither is a published `dataset_version` (see
`spark/doc/storage_convention.md`).

Same shape as `run_sql_on_iceberg`: a small PySpark script is uploaded to S3 at run time and launched
as the EMR Serverless entry point (no fat-JAR rebuild), and the driver stdout is forwarded into the
task log. The job runs there because the EMR execution role is what holds access to the Glue-backed
tables.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from airflow.sdk.definitions.param import Param

from opendatalake.lib import config
from opendatalake.lib.operators.emr import EmrServerlessJobOperator, job_name_timestamp

# Mirrors org.radiant.opendatalake.wap.iceberg.IcebergTable.LatestTag
_DEFAULT_TAG = "latest"

# The PySpark entry point, uploaded to S3 verbatim before the job launches. Kept inline so the DAG is
# the only thing to deploy. Content is fixed, so a single overwritten key is safe across runs.
_SCRIPT_KEY = "scripts/tag_latest/tag_latest.py"

_TAG_LATEST_SCRIPT = '''\
"""Point a tag (default `latest`) at the newest version branch holding data, per Iceberg table."""
import argparse
import sys

from pyspark.sql import SparkSession

MAIN_BRANCH = "main"
AUDIT_PREFIX = "audit_"


def list_tables(spark, database):
    rows = spark.sql(f"SHOW TABLES IN {database}").collect()
    return [getattr(r, "tableName", None) or r[1] for r in rows]


def version_branches(spark, full_name):
    # SparkCatalog caches metadata (30s default TTL); refresh before every ref read.
    spark.sql(f"REFRESH TABLE {full_name}")
    rows = spark.sql(f"SELECT name, snapshot_id FROM {full_name}.refs WHERE type = 'BRANCH'").collect()
    return [
        (r["name"], r["snapshot_id"])
        for r in rows
        if r["name"] != MAIN_BRANCH and not r["name"].startswith(AUDIT_PREFIX)
    ]


def snapshot_index(spark, full_name):
    # .snapshots lists every snapshot in table metadata, not just main's ancestry (.history does that),
    # so branch snapshots are in here.
    rows = spark.sql(f"SELECT snapshot_id, committed_at, summary FROM {full_name}.snapshots").collect()
    return {r["snapshot_id"]: (r["committed_at"], (r["summary"] or {}).get("total-records")) for r in rows}


def branch_row_count(spark, full_name, branch):
    # The `branch` option works on the read path (it is silently ignored on writes).
    return spark.read.option("branch", branch).table(full_name).count()


def newest_branch_with_data(spark, full_name, branches, index):
    candidates = []
    for name, snapshot_id in branches:
        if snapshot_id not in index:
            print(f"  ! branch {name}: snapshot {snapshot_id} not in .snapshots, ignored")
            continue
        committed_at, total_records = index[snapshot_id]
        # Every Iceberg write writes total-records into the summary; only count rows when it is absent.
        rows = int(total_records) if total_records is not None else branch_row_count(spark, full_name, name)
        print(f"  - branch {name}: snapshot {snapshot_id}, committed {committed_at}, {rows} row(s)")
        if rows > 0:
            candidates.append((committed_at, snapshot_id, name))
    if not candidates:
        return None
    # Ordered by commit time: version names are not comparable (dbsnp publishes GCF_000001405.40).
    committed_at, snapshot_id, name = max(candidates)
    return name, snapshot_id, committed_at


def tag_snapshot(spark, full_name, tag, snapshot_id):
    # Backticks: an all-digit or dotted ref name is not a valid bare SQL identifier.
    spark.sql(f"ALTER TABLE {full_name} CREATE OR REPLACE TAG `{tag}` AS OF VERSION {snapshot_id}")
    spark.sql(f"REFRESH TABLE {full_name}")
    confirmed = spark.sql(f"SELECT snapshot_id FROM {full_name}.refs WHERE name = '{tag}'").collect()
    if not confirmed or confirmed[0][0] != snapshot_id:
        raise RuntimeError(f"tag {tag} did not land on snapshot {snapshot_id} (got {confirmed})")


def process(spark, database, table, tag, dry_run):
    full_name = f"{database}.{table}"
    print(f"== {full_name}")

    branches = version_branches(spark, full_name)
    if not branches:
        print("  skipped: no published version branch")
        return "skipped"

    picked = newest_branch_with_data(spark, full_name, branches, snapshot_index(spark, full_name))
    if picked is None:
        print("  skipped: no version branch holds data")
        return "skipped"

    name, snapshot_id, committed_at = picked
    if dry_run:
        print(f"  dry-run: would move {tag} -> {name} (snapshot {snapshot_id}, committed {committed_at})")
        return "planned"

    tag_snapshot(spark, full_name, tag, snapshot_id)
    print(f"  tagged {tag} -> {name} (snapshot {snapshot_id}, committed {committed_at})")
    return "tagged"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--tables", default="")  # comma-separated; empty means every table in the database
    parser.add_argument("--tag", default="latest")
    parser.add_argument("--dry-run", default="false")
    args = parser.parse_args()

    dry_run = args.dry_run.strip().lower() in ("1", "true", "yes")
    requested = [t.strip() for t in args.tables.split(",") if t.strip()]

    spark = SparkSession.builder.appName("opendatalake-tag-latest").getOrCreate()
    print("===== opendatalake-tag-latest =====")
    tables = requested or list_tables(spark, args.database)
    print(f"database={args.database} tag={args.tag} dry_run={dry_run} tables={len(tables)}")

    results = {}
    failures = []
    for table in tables:
        try:
            results[table] = process(spark, args.database, table, args.tag, dry_run)
        except Exception as error:
            # A discovered table need not be Iceberg at all; only an explicitly requested one fails the job.
            print(f"  ! {type(error).__name__}: {error}")
            results[table] = "error"
            if requested:
                failures.append(table)

    print("===== summary =====")
    for table, outcome in results.items():
        print(f"{outcome:>8}  {args.database}.{table}")

    spark.stop()
    if failures:
        sys.exit(f"failed on: {', '.join(failures)}")


if __name__ == "__main__":
    main()
'''


def _script_s3_uri() -> str:
    return f"s3://{config.raw_datalake_bucket}/{_SCRIPT_KEY}"


def _params() -> dict:
    return {
        "database": Param(
            config.iceberg_database,
            type="string",
            title="Iceberg database",
            description="Database holding the tables, e.g. `reference`.",
        ),
        "tables": Param(
            "",
            type="string",
            title="Tables (comma-separated)",
            description="Empty means every table in the database. Example: `clinvar_v1,dbsnp_v1`.",
        ),
        "tag": Param(
            _DEFAULT_TAG,
            type="string",
            title="Tag name",
            description="Tag to move. Leave as `latest` unless backfilling something else.",
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="Report the branch that would be tagged, without writing the tag.",
        ),
    }


@dag(
    dag_id=f"{config.DAG_ID_PREFIX}-tag-latest",
    dag_display_name=f"{config.DAG_DISPLAY_NAME_PREFIX} - Tag latest on Iceberg",
    schedule=None,
    params=_params(),
    tags=config.DAG_DEFAULT_TAGS + [f"{config.DAG_ID_PREFIX}_{t}" for t in ["iceberg", "manual", "utility"]],
    catchup=False,
)
def tag_latest_on_iceberg():
    @task(task_display_name="[PyOp] Prepare tagging job")
    def prepare_job(params=None) -> dict:
        params = params or {}
        database = str(params.get("database") or "").strip()
        if not database:
            raise ValueError("The 'database' param is required; supply the Iceberg database to scan.")

        tables = [t.strip() for t in str(params.get("tables") or "").split(",") if t.strip()]
        # Upload the entry-point script (idempotent: fixed content, fixed key).
        S3Hook(config.s3_conn_id).load_string(
            _TAG_LATEST_SCRIPT,
            key=_SCRIPT_KEY,
            bucket_name=config.raw_datalake_bucket,
            replace=True,
        )
        # entryPointArguments must be strings; stringify here so the templated job driver stays valid.
        return {
            "database": database,
            "tables": ",".join(tables),
            "tag": str(params.get("tag") or _DEFAULT_TAG).strip(),
            "dry_run": "true" if params.get("dry_run") else "false",
        }

    prepared = prepare_job()

    # PySpark mode: entry_point is the uploaded script; the operator adds the fat JAR as spark.jars for
    # the Iceberg/Glue classes and forwards the driver logs (the per-table report) into this task's log.
    EmrServerlessJobOperator(
        task_id="tag_latest",
        task_display_name="[EMR] Tag latest on Iceberg",
        entry_point=_script_s3_uri(),
        entry_point_arguments=[
            "--database",
            prepared["database"],
            "--tables",
            prepared["tables"],
            "--tag",
            prepared["tag"],
            "--dry-run",
            prepared["dry_run"],
        ],
        # Metadata-only job, sized to the smallest workers EMR Serverless sells (1 vCPU, 2-8 GB). The
        # application's maximumCapacity (100 vCPU / 400 GB) is shared by every concurrent job, and a
        # heavy import at full scale-out reaches it on its own — gnomad_joint's maxExecutors=24 is
        # 24x(4 vCPU/16 GB) + driver = exactly 100 vCPU / 400 GB. At the EMR defaults (4 vCPU, 14 GB +
        # 10% overhead -> a 16 GB worker) this job would ask for 48 GB and be rejected while that runs;
        # at ~3 GB workers it fits in the leftover. Dynamic allocation off: one fixed executor is
        # enough to scan metadata tables, and it keeps the footprint exact rather than opportunistic.
        spark_conf={
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "1",
            "spark.driver.cores": "1",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "1",
            "spark.executor.memory": "2g",
            "spark.sql.shuffle.partitions": "8",
        },
        name=f"opendatalake-{config.environment}-tag-latest-{job_name_timestamp()}",
        waiter_delay=30,
        waiter_max_attempts=120,
    )


tag_latest_on_iceberg()
