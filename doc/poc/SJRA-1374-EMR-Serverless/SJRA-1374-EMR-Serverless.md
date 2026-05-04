# SJRA-1374 — POC: EMR Serverless for OpenDataLake

Proof-of-concept running the `radiant-open-datalake` Spark/Scala ETL on **AWS EMR Serverless**, orchestrated by Airflow, with **AWS Glue** as the Iceberg catalog. 

ClinVar is the chosen workload used in this POC, since it's available in the sources list and is of manageable size (small).

## 1. Goal

Evaluate a deployment using EMR Serverless to run the Spark-based transformations. 

In more detail, we need to validate that:

- The existing fat JAR (`org.radiant.opendatalake.ImportPublicTable`) runs unchanged on EMR Serverless.
- Airflow can submit EMR Serverless jobs, poll for completion, and surface driver logs in the task log.

Additionally, we need to collect information about:

- IAM permissions can be scoped appropriately for both the EMR execution role and the Airflow caller.
- Cost / latency profile is acceptable for the open-datalake workload.

ClinVar was chosen as the first dataset: small (~4M rows), self-contained, distributed as VCF (so it exercises Glow), and has no upstream dependencies.

## 2. Background

### 2.1 What EMR is

**Amazon EMR** (Elastic MapReduce) is AWS's managed runtime for the Hadoop/Spark ecosystem (Spark, Hive, Trino, Presto, HBase, Flink). 
AWS distributes a curated **EMR release** (e.g. `emr-7.12.0`) bundling tested versions of those engines, the JDK, S3 connectors, and Glue/Lake Formation integrations. 
We only use the Spark side.

Other releases are available here: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html

EMR offers three deployment modes:

| Mode               | What runs your job                                                                |
|--------------------|-----------------------------------------------------------------------------------|
| **EMR on EC2**     | A long-lived or transient cluster of EC2 instances (master + core + task nodes)   |
| **EMR on EKS**     | Kubernetes pods on an existing EKS cluster                                        |
| **EMR Serverless** | A managed worker pool the service provisions on demand from a logical application |

This POC uses **EMR Serverless**.

### 2.2 Spark concepts and how EMR Serverless realizes them

Plain Spark has three roles: a **driver** (orchestrates the job, holds the SparkContext), **executors** (run tasks), and a **cluster manager** (allocates resources — YARN, Kubernetes, Standalone…). 
EMR Serverless plays the role of the cluster manager and supplies both the driver and executor processes from its own pool of workers.

| Spark concept         | Plain Spark / `spark-submit`                                       | EMR on EC2                                                  | EMR Serverless                                                          |
|-----------------------|--------------------------------------------------------------------|-------------------------------------------------------------|-------------------------------------------------------------------------|
| **Cluster manager**   | YARN / Kubernetes / Standalone                                     | YARN on the EMR master node                                 | Hidden — managed by the service                                         |
| **Driver process**    | Local JVM (client mode) or a YARN AM (cluster mode)                | YARN AM on a core node                                      | One worker dedicated to the driver (4 vCPU / 14 GB default)             |
| **Executors**         | JVMs requested via `--num-executors` / dynamic allocation          | YARN containers on core/task nodes                          | Workers scaled by Spark dynamic allocation, capped by app maxCapacity   |
| **Resource unit**     | YARN container (vCPU + RAM)                                        | YARN container                                              | "Worker" — fixed bundle of vCPU + RAM + disk                            |
| **Submit endpoint**   | `spark-submit ...`                                                 | EMR `Step` API (`AddJobFlowSteps`)                          | `StartJobRun` API on an Application                                     |
| **Logs**              | stdout/stderr of the driver process                                | YARN aggregated logs in S3 + on-cluster files               | CloudWatch Logs and/or S3 (configured per job)                          |

Key consequence: a `spark-submit` invocation that runs locally maps almost 1:1 onto an EMR Serverless `StartJobRun` — the JAR, main class, and `--conf` settings are unchanged; only the resource-allocation flags differ.

### 2.3 The two EMR Serverless objects: Application and Job Run

EMR Serverless splits "the cluster" into two long-lived API objects:

- **Application** — a *template* declaring the EMR release, the engine type (`SPARK` or `HIVE`), an architecture (`x86_64` / `arm64`), VPC/subnet config, max capacity, and optional pre-initialized capacity. 
It costs nothing while idle (unless you enable pre-init). 
This POC's app is `poc-emr-opendatalake`.
- **Job Run** — a single `spark-submit` execution against an application. Carries the JAR, entry args, Spark conf, and per-job `configurationOverrides` (e.g. CloudWatch monitoring). 
Job runs are the billable unit.

```mermaid
flowchart LR
    subgraph App["EMR Serverless Application (template)"]
        direction TB
        A_REL["releaseLabel: emr-7.12.0"]
        A_ENG["engine: SPARK"]
        A_NET["VPC / subnets / SGs"]
        A_CAP["maxCapacity (cap)"]
        A_PRE["pre-init capacity (opt.)"]
    end

    JR1["Job Run #1<br/>StartJobRun"]
    JR2["Job Run #2<br/>StartJobRun"]
    JR3["Job Run #N..."]

    App --> JR1 & JR2 & JR3

    subgraph Workers["Workers provisioned per Job Run"]
        D["1 driver worker"]
        E["N executor workers<br/>(dynamic allocation)"]
    end

    JR1 -.-> Workers
```

Application states: `CREATING → CREATED → STARTING → STARTED → STOPPING → STOPPED`. The service auto-starts a stopped app on `StartJobRun` (this is why the Airflow IAM policy includes `emr-serverless:StartApplication`).

Job-run states: `SUBMITTED → PENDING → SCHEDULED → RUNNING → SUCCESS | FAILED | CANCELLED`. Failed job runs **cannot be restarted** — the operator surface is to clone-and-resubmit, optionally driven by a retry policy on the application.

### 2.4 Workers, sizing, and scaling

- A **worker** is the smallest unit of capacity — one bundle of vCPU + memory + disk. Default: 4 vCPU / 14 GB / 20 GB. You can override per role (driver / executor) at job submit time.
- A worker hosts exactly **one Spark process** (driver *or* executor); it is not a YARN node hosting multiple containers.
- Spark **dynamic allocation is on by default**. Cap it with `spark.dynamicAllocation.maxExecutors` (job-level) and `maximumCapacity` (application-level).
- Scaling is **fine-grained per stage**: the service adds workers when a stage needs more parallelism and releases them when it doesn't. Idle workers are decommissioned within seconds.
- **Pre-initialized capacity** keeps a configurable number of workers warm on the application so jobs skip the ~30 s cold-start. Costs idle compute time. Useful only for time-sensitive or interactive paths — *not* used in this POC.

### 2.5 Storage choice

EMR Serverless offers three local-disk shapes for shuffle/spill:

| Type                         | EMR release | Disk per worker     | Notes                                                                              |
|------------------------------|-------------|---------------------|------------------------------------------------------------------------------------|
| **Standard disks**           | ≤ 7.11      | 20–200 GB           | Simple; cheapest for small jobs                                                    |
| **Shuffle-optimized disks**  | 7.1.0+      | 20–2,000 GB         | High IOPS / throughput; required worker size ≥ 4 vCPU                              |
| **Serverless storage**       | 7.12+       | Auto-scaling, free  | Replaces local disk; no sizing; zero storage cost; recommended default             |

We use **serverless storage**. Engaging it requires ≥ 4 vCPU per worker — which the §5 worker-sizing trap is about.

### 2.6 Pricing model

EMR Serverless bills three meters per second, with a **1-minute minimum per worker**:

- **vCPU-hours** (≈ \$0.0526 / vCPU-hr in `us-east-1`, 2026 rates)
- **memoryGB-hours** (≈ \$0.0058 / GB-hr)
- **storageGB-hours** above 20 GB / worker; **0 with serverless storage**

There is no per-cluster, per-hour, or "EMR uplift" charge on top — workers are billed only while running. Idle applications cost nothing (unless pre-init capacity is enabled, in which case the warm pool is billed continuously).

### 2.7 What this means for our ETL

- Our Spark/Scala job (`org.radiant.opendatalake.ImportPublicTable`) is a normal `spark-submit`. It runs unchanged on EMR Serverless — only the **submission mechanism** (an Airflow operator that calls `StartJobRun`) and the **catalog wiring** (Iceberg + Glue) are EMR-specific.
- Cluster management drops out of our concerns: no cluster create/terminate operators, no AMI/bootstrap scripts, no idle-time tuning.
- The cost model is per-job — exactly what a daily/on-demand reference-data ingestion pattern wants.

> Refs: [EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html), [Spark application architecture](https://spark.apache.org/docs/latest/cluster-overview.html), [EMR release versions for Serverless](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-serverless-release-versions.html), [Pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-behavior.html#pre-init-capacity).

## 3. Architecture

![architecture.png](architecture.png)

```
Airflow (MWAA / local)
  └─ DAG: x-emr-serverless-poc
       └─ EmrServerlessStartJobWithLogsOperator (custom)
              ├─ submits spark-submit to EMR Serverless application
              │     ├─ entryPoint: fat JAR on S3 (radiant-open-datalake-spark.jar)
              │     ├─ Iceberg + Glue catalog conf
              │     └─ in-memory session catalog (avoids Hive)
              ├─ polls until terminal status
              └─ collects CloudWatch driver stdout/stderr into the Airflow task log
```

### 3.1 AWS resources provisioned

| Resource                   | Identifier                                                                          |
|----------------------------|-------------------------------------------------------------------------------------|
| EMR Serverless application | `poc-emr-opendatalake` (id `00g59743sbl50409`)                                      |
| Region / account           | `us-east-1` / `418295705741`                                                        |
| Execution role             | `arn:aws:iam::418295705741:role/service-role/AmazonEMR-ExecutionRole-1777389601404` |
| Glue database              | `opendatalake_poc`                                                                  |
| CloudWatch log group       | `/aws/emr-serverless/poc-emr-opendatalake`                                          |
| CloudWatch stream prefix   | `poc_emr`                                                                           |
| Warehouse bucket / prefix  | `s3://radiant-tst-datalake-qa/opendatalake/`                                        |
| Fat JAR location           | `s3://radiant-tst-datalake-qa/opendatalake/jars/radiant-open-datalake-spark.jar`    |

### 3.2 EMR Serverless vs EC2 

The main differences between Serverless and EC2 modes are summarized in the table below:

| Dimension                   | A. EMR Serverless                                                                             | B. EMR on EC2 transient                                                                                                  | 
|-----------------------------|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| **Model**                   | Per-job Spark app on auto-scaled pool; pay per vCPU-s / GB-s                                  | Transient cluster per job/DAG run via `EmrCreateJobFlowOperator` + `EmrAddStepsOperator` + `EmrTerminateJobFlowOperator` |
| **Sizing fit**              | Per-job right-sizing; no idle cost                                                            | One cluster shape per job;  Resize is node-based.                                                                        |
| **Startup latency**         | Cold-start in seconds; pre-init capacity for hot paths                                        | 5–8 min bootstrap per cluster                                                                                            |
| **Cost — bursty workload**  | Pay-per-use fits idle gaps                                                                    | Bootstrap × N jobs                                                                                                       |
| **Cost — sustained load**   | ~20–30% above Spot EC2 at >70% util                                                           | Cheapest (Spot core fleet)                                                                                               |
| **Spark / Iceberg version** | Spark 3.5.x + Iceberg 1.10 first-class on EMR 7.x; magic S3A committer default                | Same EMR 7.x runtime                                                                                                     |
| **Airflow integration**     | `EmrServerlessStartJobOperator` in `apache-airflow-providers-amazon` 9.12.0 (already in MWAA) | Same provider, EMR job-flow operators                                                                                    |
| **Ops burden**              | Zero cluster ops                                                                              | AMIs, bootstrap scripts, version upgrades                                                                                |
| **Debugging**               | No SSH; CloudWatch + Spark UI only; coarser executor shapes                                   | SSH, full instance control                                                                                               |
| **New platform commitment** | None                                                                                          | None                                                                                                                     |

References: 
- [EMR Serverless release notes](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-serverless-release-versions.html) 
- [EmrServerlessStartJobOperator (Airflow operator)](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr/emr_serverless.html) 
- [EMR Serverless custom images](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html)


## 4. IAM

Two distinct policies are involved: one on the **EMR execution role** (what the Spark job can do) and one on the **Airflow caller** (what Airflow can do to submit/poll). A third set of perms — for **human users hitting the AWS console** — is documented in §9.3.

### 4.1 Execution role policy

Statements:

- **CloudWatch Logs** — `CreateLogGroup`, `CreateLogStream`, `PutLogEvents` on `/aws/emr-serverless/poc-emr-opendatalake[:*]`; `DescribeLogGroups/Streams` on `*`. Without these, EMR Serverless silently drops driver logs.
- **S3** — `ListBucket` (scoped via prefix condition to `opendatalake/*`) and read/write on `s3://radiant-tst-datalake-qa/opendatalake/*`.
- **Glue read** — scoped to `opendatalake_poc`, plus `default` and `global_temp` (see §4.3).
- **Glue write** — strictly scoped to `opendatalake_poc`.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudWatchLogsForDriverAndExecutor",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:418295705741:log-group:/aws/emr-serverless/poc-emr-opendatalake",
                "arn:aws:logs:us-east-1:418295705741:log-group:/aws/emr-serverless/poc-emr-opendatalake:*"
            ]
        },
        {
            "Sid": "CloudWatchLogsDescribe",
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams"
            ],
            "Resource": "*"
        },
        {
            "Sid": "S3ListBucketScopedToOpendatalakePrefix",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::radiant-tst-datalake-qa",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "opendatalake",
                        "opendatalake/*"
                    ]
                }
            }
        },
        {
            "Sid": "S3ReadWriteOnOpendatalakeObjects",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": "arn:aws:s3:::radiant-tst-datalake-qa/opendatalake/*"
        },
        {
            "Sid": "GlueCatalogRead",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:GetUserDefinedFunctions",
                "glue:GetUserDefinedFunction",
                "glue:GetCatalogImportStatus"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:418295705741:catalog",
                "arn:aws:glue:us-east-1:418295705741:database/default",
                "arn:aws:glue:us-east-1:418295705741:database/global_temp",
                "arn:aws:glue:us-east-1:418295705741:database/opendatalake_poc",
                "arn:aws:glue:us-east-1:418295705741:table/opendatalake_poc/*",
                "arn:aws:glue:us-east-1:418295705741:userDefinedFunction/default/*",
                "arn:aws:glue:us-east-1:418295705741:userDefinedFunction/opendatalake_poc/*"
            ]
        },
        {
            "Sid": "GlueCatalogWriteOpenDatalakePocOnly",
            "Effect": "Allow",
            "Action": [
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:CreatePartition",
                "glue:UpdatePartition",
                "glue:BatchCreatePartition"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:418295705741:catalog",
                "arn:aws:glue:us-east-1:418295705741:database/opendatalake_poc",
                "arn:aws:glue:us-east-1:418295705741:table/opendatalake_poc/*"
            ]
        }
    ]
}
```

### 4.2 Airflow policy (job submission + log read)

```json
{
    "Action": [
        "emr-serverless:ListApplications",
        "emr-serverless:GetApplication",
        "emr-serverless:StartApplication",
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun"
    ],
    "Effect": "Allow",
    "Resource": "arn:aws:emr-serverless:us-east-1:418295705741:/applications/*"
},
{
    "Action": "iam:PassRole",
    "Effect": "Allow",
    "Resource": "arn:aws:iam::418295705741:role/service-role/AmazonEMR-ExecutionRole-1777389601404"
},
{
    "Effect": "Allow",
    "Action": [
        "logs:GetLogEvents",
        "logs:DescribeLogStreams",
        "logs:FilterLogEvents"
    ],
    "Resource": "arn:aws:logs:us-east-1:418295705741:log-group:/aws/emr-serverless/poc-emr-opendatalake:*"
}
```

`iam:PassRole` is mandatory — `StartJobRun` passes the execution role to EMR Serverless on Airflow's behalf.

### 4.3 IAM gotchas (learned the hard way)

- The `catalog` resource ARN is **required** in every Glue statement. Glue authorizes write actions against catalog + database + table together; dropping the catalog ARN returns `AccessDenied`. Reference: [AWS Glue resource ARNs](https://docs.aws.amazon.com/glue/latest/dg/glue-specifying-resource-arns.html).
- `database/default` is included in **read** because the AWS Glue Hive client factory (`AWSGlueDataCatalogHiveClientFactory`) probes `default` on init even when it isn't used. Without read access there, Spark fails at `SharedState` initialization. The §6.2 setting `spark.sql.catalogImplementation=in-memory` avoids this codepath, but the IAM grant remains as belt-and-suspenders.
- `glue:GetDatabases` (list) crosses resource boundaries — an AWS API quirk. It returns metadata across all DBs in the catalog regardless of resource filter; minor information leak. Iceberg does not need it for normal read/write. Drop it if list-isolation is required.
- Future maintenance ops on Iceberg (`expire_snapshots`, `remove_orphan_files`, `drop table`) need additional perms: `glue:DeleteTable`, `glue:DeletePartition`, `glue:BatchDeletePartition`, `glue:BatchDeleteTable`. Not granted in this POC.

> Ref: [Iceberg AWS Glue catalog perms](https://iceberg.apache.org/docs/latest/aws/#glue-catalog).

## 5. Worker sizing

The first job submission failed with:

```
botocore.errorfactory.ValidationException: An error occurred (ValidationException) when calling the StartJobRun operation: Serverless storage for EMR Serverless is not supported for 1 and 2 vCPU workers (drivers or executors).
```

**Cause.** The `SHUFFLE_OPTIMIZED` disk type (a.k.a. EMR Serverless "serverless storage") requires **≥4 vCPU per worker**. The original config requested 1–2 vCPU.

**Fix.** Drop explicit driver/executor sizing entirely. EMR Serverless defaults to **4 vCPU / 14 GB RAM / 20 GB disk** on both driver and executor — which satisfies the floor and is appropriate for a small workload like ClinVar.

If a future job actually needs custom sizing:

| Option | Effect |
|---|---|
| Bump to ≥4 vCPU and keep `diskType: SHUFFLE_OPTIMIZED` | Best for shuffle-heavy joins, wide variant ETL |
| Stay at 1–2 vCPU and omit `diskType` (defaults to `STANDARD`) | Cheaper for light POCs, no shuffle optimization |

> Ref: [EMR Serverless worker config](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html#spark-jobs-worker-configurations).

### 5.1 Serverless storage quick-facts

- Managed temporary storage layer for Spark intermediate data (shuffle, spill, cache); auto-scales with workload.
- In-job temporary data only — deleted when the job completes.
- **Storage is free** — you pay only for compute and memory.
- Replaces local-disk handling for Spark intermediates; avoids job failures from disk-fill.
- Lets Spark release workers sooner → reduces compute cost.
- Requires EMR release **7.12+**.

## 6. Spark configuration

### 6.1 Final `SPARK_CONF`

```python
SPARK_CONF = {
    # Cap dynamic allocation — small dataset, prevent runaway cost
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

    # Small dataset — keep shuffle partitions modest
    "spark.sql.shuffle.partitions": "16",
}
```

### 6.2 Decision rationale

| Setting                                                             | Why                                                                                                                                                                                                                                                                                                             |
|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `spark.dynamicAllocation.maxExecutors=4`                            | Caps blast radius. Default is unbounded; ClinVar fits in single-digit executors.                                                                                                                                                                                                                                |
| `spark.dynamicAllocation.initialExecutors=1`                        | Avoid over-provisioning at job start. Will scale up if shuffle stages need it.                                                                                                                                                                                                                                  |
| `spark.sql.extensions=IcebergSparkSessionExtensions`                | Required for Iceberg DDL (`MERGE INTO`, `CREATE OR REPLACE`, branch/tag ops).                                                                                                                                                                                                                                   |
| `spark.sql.catalogImplementation=in-memory`                         | **Critical.** EMR Serverless defaults to `hive`, which forces `AWSCatalogMetastoreClient` initialization and a Glue probe on `default` DB. With `in-memory`, Spark uses `InMemoryCatalog` for SessionCatalog; the Iceberg `opendatalake` catalog is unaffected. Avoids the IAM denial path entirely (see §9.4). |
| `spark.sql.catalog.opendatalake.default-namespace=opendatalake_poc` | Unqualified table refs (`spark.table("clinvar")`) resolve to `opendatalake_poc.clinvar`. Defends against IAM denial if Scala code drops the database prefix.                                                                                                                                                    |
| `spark.sql.catalog.opendatalake.glue.id=418295705741`               | Pins the Glue catalog ID. **Iceberg 1.10.x uses `glue.id`** (older versions used `glue.catalog-id`).                                                                                                                                                                                                            |
| `spark.sql.catalog.opendatalake.io-impl=S3FileIO`                   | Native S3 I/O, bypasses Hadoop's S3A. Faster for Iceberg.                                                                                                                                                                                                                                                       |
| `spark.sql.defaultCatalog=opendatalake`                             | Default catalog for unqualified table refs.                                                                                                                                                                                                                                                                     |
| `spark.sql.shuffle.partitions=16`                                   | Default 200 is overkill for small datasets.                                                                                                                                                                                                                                                                     |

> Refs: [Iceberg Spark catalog config](https://iceberg.apache.org/docs/latest/spark-configuration/#catalog-configuration), [Iceberg AWS Glue catalog (1.10.0)](https://iceberg.apache.org/docs/1.10.0/aws/#glue-catalog).

### 6.3 Configuration tried and dropped

- **`--packages org.apache.iceberg:...`** — initial approach. Adds 30–90 s of Ivy resolve at every job start and requires Maven Central egress from the VPC. Replaced after confirming the project ships a fat JAR with all deps bundled.
- **`--jars s3://.../iceberg-*.jar`** — secondary approach with pre-uploaded jars. Also dropped after fat JAR confirmation.
- **`spark.serializer=KryoSerializer`** — initially recommended as an "Iceberg best practice", but on verification this is a generic Spark recommendation (network-intensive workloads), not Iceberg-specific. ClinVar is small + low-shuffle, so the gain is negligible. Skipped.
- **`spark.driver.extraJavaOptions=-Duser.language=en -Duser.country=US`** — Glow VCF reader locale workaround. Initially included, then removed under the mistaken assumption that ClinVar was TSV. ClinVar is in fact distributed as VCF (NCBI publishes `clinvar.vcf.gz` for both GRCh37/38), so Glow is exercised. Currently omitted; revisit if Glow throws locale-sensitive parse errors. See §11 for the quoted-value pattern.

### 6.4 Application-level configuration (current)

```json
[
  {
    "classification": "spark-defaults",
    "properties": {
      "spark.aws.serverlessStorage.enabled": "true",
      "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
  }
]
```

The `hive.metastore.client.factory.class` only matters if the Hive client is constructed. With job-level `spark.sql.catalogImplementation=in-memory`, the Hive client is never created and the factory class is never invoked. Kept for compatibility with any future job that needs a hive-backed SessionCatalog.

## 7. DAG implementation

`radiant/dags/emr-poc.py`:

```python
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

ENTRY_CLASS = "org.radiant.opendatalake.ImportPublicTable"
ENTRY_ARGS = [
    "clinvar",
    "--config", "config/poc.conf",
    "--steps", "default",
    "--app-name", "clinvar-poc",
]

SPARK_CONF = { ... }  # see §6.1

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
    catchup=False,
    tags=["emr", "serverless", "opendatalake", "poc", "manual"],
) as dag:
    run_emr_job = EmrServerlessStartJobWithLogsOperator(
        task_id="start_clinvar_job",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        name="poc_emr_clinvar_1.0.0_{{ ts_nodash }}",
        job_driver=JOB_DRIVER,
        cloudwatch_log_group=LOG_GROUP_NAME,
        cloudwatch_log_stream_prefix=LOG_STREAM_PREFIX,
        cloudwatch_region=AWS_REGION,
        enable_application_ui_links=True,
        waiter_delay=30,
        waiter_max_attempts=60,
        pipe_stderr=True,
    )
```

### 7.1 Notes

- `pipe_stderr=True` is essential — Spark driver logs go to **stderr** (log4j console appender), not stdout. Without this, the operator looks at an empty/missing stdout stream and reports `Stream not found`.
- `waiter_delay=30, waiter_max_attempts=60` ⇒ 30-minute timeout. Re-tune for larger datasets.
- `enable_application_ui_links=True` exposes the Spark UI / driver-log dashboard URL in the operator's task log.
- `config/poc.conf` is referenced as a relative path. It must be loadable from the JAR classpath (e.g. via `Typesafe ConfigFactory.load("poc.conf")`) — EMR Serverless workers don't have it on a filesystem path.

## 8. Custom operator — `EmrServerlessStartJobWithLogsOperator`

Location: `radiant/dags/operators/emr.py`. Subclasses `airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator`.

### 8.1 Responsibilities

1. **Inject CloudWatch monitoring config** into the `StartJobRun` request unless the caller already set one. Done by `_merge_monitoring`, which builds `configuration_overrides.monitoringConfiguration.cloudWatchLoggingConfiguration` with `enabled=true`, the configured log group, and stream prefix.
2. **Wait for job completion** (default `wait_for_completion=True`).
3. **Forward driver logs** — after the job exits (success **or** failure), read the `SPARK_DRIVER` stdout (and optionally stderr) streams from CloudWatch and forward them line-by-line into the Airflow task log. Done in a `finally` block so logs surface even on job failure.

```mermaid
sequenceDiagram
    participant T as Airflow task
    participant OP as EmrServerlessStartJobWithLogsOperator
    participant EMR as EMR Serverless
    participant CW as CloudWatch Logs

    T->>OP: execute(context)
    Note over OP: _merge_monitoring()<br/>injects logGroupName + prefix
    OP->>EMR: StartJobRun(jobDriver, configurationOverrides)
    EMR-->>OP: jobRunId

    loop wait_for_completion
        OP->>EMR: GetJobRun(jobRunId)
        EMR-->>OP: state (PENDING / RUNNING / SUCCESS / FAILED)
    end

    Note over OP: try block ends<br/>(success or AirflowException)

    rect rgba(200,200,200,0.15)
    Note over OP,CW: finally: _forward_driver_logs()
    OP->>CW: GetLogEvents(SPARK_DRIVER/stdout)
    CW-->>OP: events
    OP->>T: log.info(line) for each event
    alt pipe_stderr=True
        OP->>CW: GetLogEvents(SPARK_DRIVER/stderr)
        CW-->>OP: events
        OP->>T: log.info(line) for each event
    end
    end

    OP-->>T: return / re-raise
```

### 8.2 CloudWatch stream naming

Stream path constructed by EMR Serverless:

```
{stream_prefix}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/{stdout|stderr}
```

For this POC, the stderr stream lands at:
`poc_emr/applications/00g59743sbl50409/jobs/<job_id>/SPARK_DRIVER/stderr`.

Three streams are created per job run: driver stderr, driver stdout, and `job-metadata-log`.

### 8.3 Constructor knobs

| Param | Default | Purpose |
|---|---|---|
| `cloudwatch_log_group` | required | Log group name |
| `cloudwatch_log_stream_prefix` | `None` | Namespace within the log group |
| `cloudwatch_region` | `None` | Passed to `AwsLogsHook`; falls back to boto/AWS default |
| `pipe_stderr` | `False` | Also fetch stderr. **Should be `True` for Spark.** |

### 8.4 Source

```python
import logging
from typing import Any

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.utils.context import Context


class EmrServerlessStartJobWithLogsOperator(EmrServerlessStartJobOperator):
    """EMR Serverless start-job operator that pipes Spark driver logs into the Airflow task log.

    Injects a CloudWatch monitoring config into the EMR job request (unless caller already set
    one), runs the job to completion, then forwards SPARK_DRIVER stdout/stderr from CloudWatch
    Logs into the task log — even on job failure.
    """

    template_fields = (
        *EmrServerlessStartJobOperator.template_fields,
        "cloudwatch_log_group",
        "cloudwatch_log_stream_prefix",
    )

    def __init__(
        self,
        *,
        cloudwatch_log_group: str,
        cloudwatch_log_stream_prefix: str | None = None,
        cloudwatch_region: str | None = None,
        pipe_stderr: bool = False,
        **kwargs,
    ):
        kwargs.setdefault("wait_for_completion", True)
        kwargs["configuration_overrides"] = self._merge_monitoring(
            kwargs.get("configuration_overrides"),
            cloudwatch_log_group,
            cloudwatch_log_stream_prefix,
        )
        super().__init__(**kwargs)
        self.cloudwatch_log_group = cloudwatch_log_group
        self.cloudwatch_log_stream_prefix = cloudwatch_log_stream_prefix
        self.cloudwatch_region = cloudwatch_region
        self.pipe_stderr = pipe_stderr

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

    def execute(self, context: Context) -> Any:
        try:
            return super().execute(context)
        finally:
            self._forward_driver_logs()

    def _forward_driver_logs(self) -> None:
        log = logging.getLogger("airflow.task")
        job_run_id = getattr(self, "job_id", None)
        if not job_run_id:
            log.warning("No job_run_id available; skipping driver log forwarding.")
            return

        hook = AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.cloudwatch_region)
        base = f"applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER"
        if self.cloudwatch_log_stream_prefix:
            base = f"{self.cloudwatch_log_stream_prefix}/{base}"
        not_found = hook.get_conn().exceptions.ResourceNotFoundException

        kinds = ["stdout"]
        if self.pipe_stderr:
            kinds.append("stderr")

        for kind in kinds:
            stream = f"{base}/{kind}"
            log.info("===== SPARK_DRIVER/%s (%s) =====", kind, stream)
            try:
                empty = True
                for evt in hook.get_log_events(log_group=self.cloudwatch_log_group, log_stream_name=stream):
                    log.info(evt["message"])
                    empty = False
                if empty:
                    log.info("(no events)")
            except not_found:
                log.warning("Stream not found: %s", stream)
```

## 9. Issues encountered (chronological)

### 9.1 Worker sizing (`ValidationException`)

See §5. Fixed by removing custom worker sizing and relying on defaults.

### 9.2 Logs missing in Airflow task log

The first successful run reported `Stream not found: .../SPARK_DRIVER/stdout`. Two contributing causes:

- The operator's default `pipe_stderr=False` only checked the `stdout` stream; Spark driver writes to `stderr`. Fixed by setting `pipe_stderr=True` in the DAG.
- Possible secondary cause: log group did not exist or execution role lacked CloudWatch perms. Confirmed log group exists; execution-role IAM updated (§4.1).

### 9.3 Console 401 fetching driver UI

Manually accessing EMR Serverless logs via the AWS console returned HTTP 401. Caused by the **caller IAM principal** (the user/SSO role logged into the console) lacking `emr-serverless:GetDashboardForJobRun`. Required perms on the caller side:

```json
{
    "Effect": "Allow",
    "Action": [
        "emr-serverless:GetDashboardForJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:ListJobRuns",
        "emr-serverless:GetApplication"
    ],
    "Resource": [
        "arn:aws:emr-serverless:us-east-1:418295705741:/applications/00g59743sbl50409",
        "arn:aws:emr-serverless:us-east-1:418295705741:/applications/00g59743sbl50409/jobruns/*"
    ]
}
```

### 9.4 `glue:GetDatabase` denied on `default` DB

After a successful Iceberg write (4,185,298 records committed to `opendatalake_poc.clinvar`, snapshot `1556577534404968890`), the read-back path threw:

```
software.amazon.awssdk.services.glue.model.AccessDeniedException: User: arn:aws:sts::418295705741:assumed-role/AmazonEMR-ExecutionRole-1777389601404/...
is not authorized to perform: glue:GetDatabase on resource: arn:aws:glue:us-east-1:418295705741:database/default
```

**Root cause.** Spark's `SessionCatalog` (separate from the Iceberg `opendatalake` catalog) lazily initializes the Hive metastore client when `spark.table(...)` is called. EMR Serverless defaults to `spark.sql.catalogImplementation=hive`, which routes the call through `AWSCatalogMetastoreClient`. The client probes `default` DB existence on init via `doesDefaultDBExist`, hitting Glue `GetDatabase`. IAM only allowed `database/opendatalake_poc`.

**Fix.** Set `spark.sql.catalogImplementation=in-memory` in the job's SPARK_CONF. Spark uses `InMemoryCatalog` for SessionCatalog, the Hive client is never instantiated, and the `default` probe never happens. The Iceberg `opendatalake` catalog is independent and continues to work.

**Belt-and-suspenders.** Also added `database/default` to the IAM read resources in case future EMR runtime changes invoke the Hive shim despite `in-memory`.

```mermaid
flowchart TD
    APP["Spark application starts"]
    DECISION{"spark.sql.<br/>catalogImplementation"}

    HIVE["= hive (EMR default)"]
    INMEM["= in-memory (our fix)"]

    SHARED["SharedState init"]
    HIVECLI["AWSCatalogMetastoreClient<br/>doesDefaultDBExist()"]
    GLUEPROBE["glue:GetDatabase<br/>on database/default"]
    DENY["AccessDeniedException<br/>(IAM only allows opendatalake_poc)"]

    INMEMCAT["InMemoryCatalog<br/>(no Hive shim, no Glue probe)"]
    ICEBERG["Iceberg 'opendatalake' catalog<br/>handles all writes/reads<br/>via SparkCatalog + GlueCatalog"]

    APP --> DECISION
    DECISION -->|hive| HIVE --> SHARED --> HIVECLI --> GLUEPROBE --> DENY
    DECISION -->|in-memory| INMEM --> INMEMCAT --> ICEBERG

    style DENY fill:#fdd,stroke:#c00
    style ICEBERG fill:#dfd,stroke:#080
```

## 10. Successful run — ClinVar

Driver-log timing for the run that committed successfully:

| Stage | Duration |
|---|---|
| Pending → Scheduled | ~30 s |
| Scheduled → Running | ~30 s |
| VCF header parsing (Job 0, 12 tasks) | 8.2 s |
| Variant transform (ShuffleMapStage 2, 12 tasks) | 26.4 s |
| Iceberg write (Job 2, 1 task, coalesced) | 92.3 s |
| Iceberg commit | 371 ms |
| **Total job time** | **~2 min 30 s** |

Output:

- Records written: **4,185,298**
- Files written: **1** Parquet data file
- Total size: **~165 MB** (165,060,724 bytes)
- Table: `opendatalake.opendatalake_poc.clinvar`
- Snapshot ID: `1556577534404968890`
- Storage location: `s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar`
- Engine: `Spark 3.5.6-amzn-1`, Java 17

### 10.1 Cost reading

Resource utilization on a representative short run (2 min 41 s, *failed* job before the fix — kept for billing illustration only):

| Metric | Total used | Billed |
|---|---|---|
| vCPU-hours | 0.041 | 0.067 |
| memoryGB-hours | 0.164 | 0.267 |
| storageGB-hours | 0.206 | 0 |

EMR Serverless bills *billed* utilization separately from *total*. Billed values reflect the **1-min minimum + brief scaling overhead**. Storage is **free** (serverless storage). At ~$0.0526/vCPU-hr + ~$0.0058/GB-hr (us-east-1, 2026 rates), this run cost ≈ **$0.005**.

## 11. Open items / follow-ups

- [ ] Confirm the next run is fully clean (no `default` DB error) after `spark.sql.catalogImplementation=in-memory` was added.
- [ ] Re-add Glow locale opts (`-Duser.language=en -Duser.country=US`) **with quoted values** if any locale-sensitive VCF parsing surfaces. Quoting is needed because spark-submit splits on whitespace; suggested pattern:
  ```python
  [f'--conf {k}={v}' if " " not in v else f'--conf "{k}={v}"' for k, v in SPARK_CONF.items()]
  ```
- [ ] Add Iceberg maintenance perms to IAM if the POC will run `expire_snapshots` / `remove_orphan_files` (`glue:DeleteTable`, `glue:DeletePartition`, `glue:BatchDeletePartition`, `glue:BatchDeleteTable`).
- [ ] Decide whether `glue:GetDatabases` (list) stays in the read policy. Returns metadata across all DBs in the catalog regardless of resource filter — minor information leak.
- [ ] Consider building a custom EMR Serverless image with the fat JAR and common dependencies pre-baked, eliminating the per-job S3 entryPoint download.
- [ ] **Cost monitoring:** tag application + jobs (`Project=radiant`, `Env=qa`, `Owner=...`) so EMR Serverless costs land in the right cost-allocation buckets.
- [ ] **Productionization:** replace `dag_id="x-emr-serverless-poc"` and `tags=[..., "poc", "manual"]` with proper naming + scheduling once promoted.
- [ ] Verify the `config/poc.conf` resolution path in `ImportPublicTable` is classpath-based (not filesystem); document expected config layout.
- [ ] Repeat the run with at least one additional dataset (Ensembl, gnomAD?) to validate the pattern generalizes.
- [ ] Decide whether `enable_application_ui_links=True` stays in prod or is restricted to staging.

## 12. Best-practice notes (AWS Big Data Blog — Top 10)

Selected items relevant to this POC. > Ref: [Top 10 best practices for Amazon EMR Serverless](https://aws.amazon.com/blogs/big-data/top-10-best-practices-for-amazon-emr-serverless/).

- **Reuse applications.** Applications are cluster templates; without pre-initialized capacity, workers are released immediately on job completion. Pre-initialized capacity is only useful for time-sensitive jobs, interactive analytics, or high-frequency pipelines.
- **Graviton.** ARM-based workers offer better performance/price; consider for prod.
- **Start with defaults, right-size by workload.** Identify the vCPU:memory ratio (memory per core) before deviating from the 4 vCPU / 16 GB default.
- **T-shirt-size scaling.** Set the upper bound via job-level `spark.dynamicAllocation.maxExecutors` *and* application-level max capacity. Suggested starting points: small (50), medium (200), large (500).
- **Storage choice.** EMR 7.12+ → use serverless storage (free, auto-scaling, recommended). Standard disks (≤7.11) for small workloads; shuffle-optimized for multi-TB ETL.
- **Multi-AZ resiliency** is automatic when pre-initialized capacity is *not* enabled. Configure retry policy for job resiliency.
- **VPC integration.** Each worker uses 1 IP per subnet — plan IP space. Use S3 Gateway endpoints in private subnets to avoid NAT data-transfer cost. Manage AWS Config costs via resource exclusions/tagging (each worker creates an ENI record).
- **Concurrency control** (EMR 7.0.0+): `--scheduler-configuration '{"maxConcurrentRuns": 5, "queueTimeoutMinutes": 30}'`.
- **Account-level guard rail.** Use the *Max concurrent vCPUs per account* service quota to prevent cross-app spikes.
- **Monitor.** CloudWatch / Prometheus / Grafana — track completion times, success rate, worker utilization, scaling events, shuffle volumes, memory.

## 13. Key takeaways

- **Pattern works.** Existing fat JAR + `ImportPublicTable` runs unmodified on EMR Serverless. ClinVar imports in ~2.5 min for ~$0.005.
- **Storage choice.** EMR 7.12+ → serverless storage is the right default (no cost, auto-scales).
- **Cost model.** Billed > total utilization (1-min minimum); storage free; expect cents per small job.
- **Airflow integration.** Runtime role + `iam:PassRole` + CloudWatch log read; stream driver logs to CloudWatch for Airflow UI visibility. Custom operator handles the "stream stderr into task log" gap.
- **Catalog.** Glue write strictly scoped to `opendatalake_poc`; reads broader (`default`, `global_temp`) to satisfy Hive-shim probes.
- **Watch out.** `spark.sql.catalogImplementation=in-memory` is non-obvious but mandatory unless you want to grant Glue read on `default`. Worker sizing must respect the ≥4 vCPU floor when using shuffle-optimized disks. Failed jobs cannot be restarted — clone & resubmit (or rely on the configured retry policy).

## 14. References

- [EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- [EMR Serverless worker config](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html#spark-jobs-worker-configurations)
- [EMR Serverless CloudWatch logs](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-log-storage.html#jobs-log-storage-cw)
- [EMR Serverless IAM examples](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/security_iam_id-based-policy-examples.html)
- [Job concurrency & queuing](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/applications-concurrency-queuing.html)
- [Application max capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/app-behavior.html#max-capacity)
- [AWS Glue resource ARNs](https://docs.aws.amazon.com/glue/latest/dg/glue-specifying-resource-arns.html)
- [Iceberg AWS Glue catalog (1.10.0)](https://iceberg.apache.org/docs/1.10.0/aws/#glue-catalog)
- [Iceberg Spark catalog config](https://iceberg.apache.org/docs/latest/spark-configuration/#catalog-configuration)
- [Glow getting started](https://glow.readthedocs.io/en/latest/getting-started.html)
- [ClinVar VCF FTP (NCBI)](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/)
- [Spark tuning — Kryo](https://spark.apache.org/docs/latest/tuning.html#data-serialization)
- [VPC S3 Gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)
- [Top 10 best practices for EMR Serverless](https://aws.amazon.com/blogs/big-data/top-10-best-practices-for-amazon-emr-serverless/)

## Appendix: Airflow logs

The following is an example of the Airflow task log for a run with `pipe_stderr=True`, showing the operator successfully reading and forwarding both stdout and stderr from the CloudWatch driver logs:

(In this example, the job doesn't output any `stdout` events, but does for `stderr`, hence the "Stream not found" warning for the `stdout` stream and the presence of events under `stderr`.)

```
*** Reading remote log from Cloudwatch log_group: airflow-radiant-tst-qa-airflow-Task log_stream: dag_id=x-emr-serverless-poc/run_id=scheduled__2026-05-03T00_00_00+00_00/task_id=start_clinvar_job/attempt=1.log.
[2026-05-04, 00:00:03 UTC] {local_task_job_runner.py:123} ▼ Pre task execution logs
[2026-05-04, 00:00:03 UTC] {taskinstance.py:2613} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: x-emr-serverless-poc.start_clinvar_job scheduled__2026-05-03T00:00:00+00:00 [queued]>
[2026-05-04, 00:00:03 UTC] {taskinstance.py:2613} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: x-emr-serverless-poc.start_clinvar_job scheduled__2026-05-03T00:00:00+00:00 [queued]>
[2026-05-04, 00:00:03 UTC] {taskinstance.py:2866} INFO - Starting attempt 1 of 1
[2026-05-04, 00:00:03 UTC] {taskinstance.py:2889} INFO - Executing <Task(EmrServerlessStartJobWithLogsOperator): start_clinvar_job> on 2026-05-03 00:00:00+00:00
[2026-05-04, 00:00:03 UTC] {standard_task_runner.py:72} INFO - Started process 22116 to run task
[2026-05-04, 00:00:03 UTC] {standard_task_runner.py:104} INFO - Running: ['airflow', 'tasks', 'run', 'x-emr-serverless-poc', 'start_clinvar_job', 'scheduled__2026-05-03T00:00:00+00:00', '--job-id', '14226', '--raw', '--subdir', 'DAGS_FOLDER/radiant/dags/emr-poc.py', '--cfg-path', '/tmp/tmpt35uza2g']
[2026-05-04, 00:00:03 UTC] {standard_task_runner.py:105} INFO - Job 14226: Subtask start_clinvar_job
[2026-05-04, 00:00:04 UTC] {taskinstance.py:3132} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='poc-emr' AIRFLOW_CTX_DAG_ID='x-emr-serverless-poc' AIRFLOW_CTX_TASK_ID='start_clinvar_job' AIRFLOW_CTX_EXECUTION_DATE='2026-05-03T00:00:00+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='scheduled__2026-05-03T00:00:00+00:00'
[2026-05-04, 00:00:04 UTC] {taskinstance.py:731} ▲▲▲ Log group end
[2026-05-04, 00:00:04 UTC] {baseoperator.py:416} WARNING - EmrServerlessStartJobWithLogsOperator.execute cannot be called outside TaskInstance!
[2026-05-04, 00:00:04 UTC] {base.py:84} INFO - Retrieving connection 'aws_default'
[2026-05-04, 00:00:05 UTC] {emr.py:1177} INFO - Application state is STOPPED
[2026-05-04, 00:00:05 UTC] {emr.py:1178} INFO - Starting application 00g59743sbl50409
[2026-05-04, 00:00:05 UTC] {waiter_with_logging.py:88} INFO - Serverless Application status is: STARTING - User initiated.
[2026-05-04, 00:00:35 UTC] {emr.py:1201} INFO - Starting job on Application: 00g59743sbl50409
[2026-05-04, 00:00:35 UTC] {emr.py:1221} INFO - EMR serverless job started: 00g5dhds4t4hvo0b
[2026-05-04, 00:00:36 UTC] {emr.py:1391} INFO - CloudWatch logs available at: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/%2Faws%2Femr-serverless%2Fpoc-emr-opendatalake$3FlogStreamNameFilter$3Dpoc_emr%2Fapplications%2F00g59743sbl50409%2Fjobs%2F00g5dhds4t4hvo0b
[2026-05-04, 00:00:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: PENDING - The JobRun is pending scheduling.
[2026-05-04, 00:01:06 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: SCHEDULED - The job has been scheduled and is acquiring resources to run.
[2026-05-04, 00:01:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:02:06 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:02:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:03:06 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:03:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:04:06 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:04:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:05:06 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:05:36 UTC] {waiter_with_logging.py:88} INFO - Serverless Job status is: RUNNING
[2026-05-04, 00:06:06 UTC] {base.py:84} INFO - Retrieving connection 'aws_default'
[2026-05-04, 00:06:07 UTC] {emr.py:82} INFO - ===== SPARK_DRIVER/stdout (poc_emr/applications/00g59743sbl50409/jobs/00g5dhds4t4hvo0b/SPARK_DRIVER/stdout) =====
[2026-05-04, 00:06:07 UTC] {emr.py:91} WARNING - Stream not found: poc_emr/applications/00g59743sbl50409/jobs/00g5dhds4t4hvo0b/SPARK_DRIVER/stdout
[2026-05-04, 00:06:07 UTC] {emr.py:82} INFO - ===== SPARK_DRIVER/stderr (poc_emr/applications/00g59743sbl50409/jobs/00g5dhds4t4hvo0b/SPARK_DRIVER/stderr) =====
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:37 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO RuntimeETLContext: Loading config file: [config/poc.conf]
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO HiveConf: Found configuration file file:/etc/spark/conf/hive-site.xml
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO EMRParamSideChannel: Setting FGAC mode to false
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO SparkContext: Running Spark version 3.5.6-amzn-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO SparkContext: OS info Linux, 6.1.166, amd64
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO SparkContext: Java version 17.0.17
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 WARN SparkConf: Note that spark.local.dir will be overridden by the value set by the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone/kubernetes and LOCAL_DIRS in YARN).
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceUtils: ==============================================================
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceUtils: No custom resources configured for spark.driver.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceUtils: ==============================================================
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO SparkContext: Submitted application: clinvar-poc
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(executorType -> name: executorType, amount: 1, script: , vendor: , cores -> name: cores, amount: 4, script: , vendor: , memory -> name: memory, amount: 14336, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfile: Limiting resource is cpus at 4 tasks per executor
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfileManager: Added ResourceProfile id: 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfile: User executor ResourceProfile created, executor resources: Map(executorType -> name: executorType, amount: 1, script: , vendor: , cores -> name: cores, amount: 4, script: , vendor: , memory -> name: memory, amount: 14336, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfile: Limiting resource is cpus at 4 tasks per executor
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:54 INFO ResourceProfileManager: Added ResourceProfile id: 1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SecurityManager: Changing view acls to: hadoop
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SecurityManager: Changing modify acls to: hadoop
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SecurityManager: Changing view acls groups to: 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SecurityManager: Changing modify acls groups to: 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SecurityManager: SecurityManager: authentication enabled; ui acls disabled; users with view permissions: hadoop; groups with view permissions: EMPTY; users with modify permissions: hadoop; groups with modify permissions: EMPTY
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO Utils: Successfully started service 'sparkDriver' on port 38819.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SparkEnv: Registering MapOutputTracker
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SparkEnv: Registering BlockManagerMaster
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO DiskBlockManager: Created local directory at /tmp/membrain/blockmgr-fb3128eb-1d69-446e-b4be-a87a18fb5b79
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO MemoryStore: MemoryStore started with capacity 8.2 GiB
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SparkEnv: Registering OutputCommitCoordinator
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SubResultCacheManager: Sub-result caches are disabled.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO Utils: Successfully started service 'SparkUI' on port 4040.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO SparkContext: Added JAR s3://radiant-tst-datalake-qa/opendatalake/jars/radiant-open-datalake-spark.jar at s3://radiant-tst-datalake-qa/opendatalake/jars/radiant-open-datalake-spark.jar with timestamp 1777852914944
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO Utils: Using effectiveMaxExecutors = 4, (spark.dynamicAllocation.maxExecutors * spark.dynamicAllocation.maxExecutorsRatio)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO Utils: Using initial executors = 3, min of effectiveMaxExecutors, (max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO ExecutorContainerAllocator: Set total expected execs to {0=3}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34121.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO NettyBlockTransferService: Server created on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc], 34121, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 with 8.2 GiB RAM, BlockManagerId(driver, [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc], 34121, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc], 34121, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc], 34121, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO ExecutorContainerAllocator: Going to request 3 executors for ResourceProfile Id: 0, target: 3 already provisioned: 0.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(1, 2, 3)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO TimeBasedRotatingEventLogFilesWriter: rotationIntervalInSeconds = 300, eventFileMinSize = 1048576, maxFilesToRetain = 2
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:55 INFO TimeBasedRotatingEventLogFilesWriter: Logging events to file:/var/log/spark/apps/eventlog_v2_00g5dhds4t4hvo0b/00g5dhds4t4hvo0b.inprogress
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:56 INFO Utils: Using effectiveMaxExecutors = 4, (spark.dynamicAllocation.maxExecutors * spark.dynamicAllocation.maxExecutorsRatio)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:56 INFO Utils: Using initial executors = 3, min of effectiveMaxExecutors, (max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:56 INFO ExecutorContainerAllocator: Set total expected execs to {0=3}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:01:56 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(2 -> 84cef824-b7b9-f726-cc7c-a08c171b9f9c, 1 -> e0cef824-b7cf-0d81-f356-56f59498d6ae, 3 -> bccef824-b7c2-1a0c-fc2d-4ad596b9d274)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951:42876
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f18:17c0:7100:2078:77fe:47d:901f:56428
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951:42886) with ID 3,  ResourceProfileId 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO ExecutorMonitor: New executor 3 has registered (new total is 1)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 with 7.9 GiB RAM, BlockManagerId(3, [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], 33585, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f18:17c0:7100:2078:77fe:47d:901f:56440) with ID 1,  ResourceProfileId 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:12 INFO ExecutorMonitor: New executor 1 has registered (new total is 2)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 with 7.9 GiB RAM, BlockManagerId(1, [2600:1f18:17c0:7100:2078:77fe:47d:901f], 35955, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f18:17c0:7100:e040:979f:d721:132b:57660
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f18:17c0:7100:e040:979f:d721:132b:57674) with ID 2,  ResourceProfileId 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO ExecutorMonitor: New executor 2 has registered (new total is 3)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO EmrServerlessClusterSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.8
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:13 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 with 7.9 GiB RAM, BlockManagerId(2, [2600:1f18:17c0:7100:e040:979f:d721:132b], 39741, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO Clinvar: RUN steps: 		 extract -> transform -> load -> publish
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO Clinvar: RUN lastRunValue: 	 None
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO Clinvar: RUN currentRunValue: 	 None
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO SharedState: Warehouse path is 'file:/home/hadoop/spark-warehouse'.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:15 INFO InMemoryFileIndex: It took 49 ms to list leaf files for 1 paths.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO SparkContext: Starting job: collect at VCFHeaderUtils.scala:130
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO elasticshuffle/openshuffle: registering shuffle 87f3c18b-bf9c-453a-a0aa-d0ada79ca157 with 12 streams and 12 queues on aggregator [2600:1f18:3be7:8903:8e71:c2f7:5a35:ad8b]:9200
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Registering RDD 3 (keyBy at VCFHeaderUtils.scala:129) as input to shuffle 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Got job 0 (collect at VCFHeaderUtils.scala:130) with 12 output partitions
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Final stage: ResultStage 1 (collect at VCFHeaderUtils.scala:130)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 0)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at keyBy at VCFHeaderUtils.scala:129), which has no missing parents
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 136.3 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 51.5 KiB, actual size: 51.5 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 (size: 51.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1721
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO DAGScheduler: Submitting 12 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at keyBy at VCFHeaderUtils.scala:129) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:16 INFO TaskSchedulerImpl: Adding task set 0.0 with 12 tasks resource profile 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:21 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 0, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:21 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 1, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:21 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 2, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 3, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 4, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 5, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 6, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 7) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 7, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 8) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 8, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 9) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 9, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 10.0 in stage 0.0 (TID 10) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 10, PROCESS_LOCAL, 9335 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO TaskSetManager: Starting task 11.0 in stage 0.0 (TID 11) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 11, PROCESS_LOCAL, 9407 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:22 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 1367 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (1/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 1369 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (2/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1385 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (3/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 1371 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (4/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 1374 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (5/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 7) in 1374 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (6/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 1378 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (7/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 1381 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (8/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO ExecutorContainerAllocator: Set total expected execs to {0=1}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 10.0 in stage 0.0 (TID 10) in 1381 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (9/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 8) in 1383 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (10/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:23 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 9) in 1384 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (11/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 11.0 in stage 0.0 (TID 11) in 1589 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (12/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: ShuffleMapStage 0 (keyBy at VCFHeaderUtils.scala:129) finished in 7.578 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: looking for newly runnable stages
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: running: Set()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: waiting: Set(ResultStage 1)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: failed: Set()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[5] at values at VCFHeaderUtils.scala:130), which has no missing parents
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 6.4 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.5 KiB, actual size: 3.5 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 (size: 3.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1721
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: Submitting 12 missing tasks from ResultStage 1 (MapPartitionsRDD[5] at values at VCFHeaderUtils.scala:130) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSchedulerImpl: Adding task set 1.0 with 12 tasks resource profile 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 12) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 0, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 13) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 1, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 14) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 2, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 15) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 3, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 16) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 4, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 17) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 5, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 18) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 6, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 19) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 7, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 8.0 in stage 1.0 (TID 20) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 8, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 9.0 in stage 1.0 (TID 21) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 9, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 10.0 in stage 1.0 (TID 22) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 10, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Starting task 11.0 in stage 1.0 (TID 23) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 11, ANY, 9168 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 11.0 in stage 1.0 (TID 23) in 289 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (1/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 22) in 291 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (2/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 13) in 298 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (3/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 8.0 in stage 1.0 (TID 20) in 294 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (4/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 12) in 302 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (5/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 15) in 300 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (6/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 7.0 in stage 1.0 (TID 19) in 298 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (7/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 17) in 301 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (8/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 14) in 302 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (9/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 9.0 in stage 1.0 (TID 21) in 298 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (10/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 16) in 303 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (11/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSetManager: Finished task 6.0 in stage 1.0 (TID 18) in 302 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (12/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: ResultStage 1 (collect at VCFHeaderUtils.scala:130) finished in 0.335 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO DAGScheduler: Job 0 finished: collect at VCFHeaderUtils.scala:130, took 8.380157 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:24 INFO ExecutorContainerAllocator: Set total expected execs to {0=0}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:25 INFO elasticshuffle: Unregistering shuffle 0 ...
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:25 INFO elasticshuffle: calling unregisterShuffle for 87f3c18b-bf9c-453a-a0aa-d0ada79ca157 on the driver
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:25 INFO elasticshuffle/openshuffle: Deleting shuffle 87f3c18b-bf9c-453a-a0aa-d0ada79ca157
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:25 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 in memory (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 in memory (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 in memory (size: 3.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 in memory (size: 3.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 in memory (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 in memory (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 in memory (size: 51.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 in memory (size: 51.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO SQLExecution: Generating and posting SparkListenerSQLExecutionObfuscatedInfo...
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:26 INFO SQLExecution: Posted SparkListenerSQLExecutionObfuscatedInfo in 9 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO GlueUtil: Not use extensions: table Table(Name=clinvar, DatabaseName=opendatalake_poc, CreateTime=2026-05-01T15:30:37Z, UpdateTime=2026-05-03T00:05:27Z, Retention=0, StorageDescriptor=StorageDescriptor(Columns=[Column(Name=chromosome, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=1, iceberg.field.optional=true}), Column(Name=start, Type=bigint, Parameters={iceberg.field.current=true, iceberg.field.id=2, iceberg.field.optional=true}), Column(Name=end, Type=bigint, Parameters={iceberg.field.current=true, iceberg.field.id=3, iceberg.field.optional=true}), Column(Name=reference, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=4, iceberg.field.optional=true}), Column(Name=alternate, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=5, iceberg.field.optional=true}), Column(Name=interpretations, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=6, iceberg.field.optional=true}), Column(Name=name, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=7, iceberg.field.optional=true}), Column(Name=clin_sig, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=8, iceberg.field.optional=true}), Column(Name=clin_sig_conflict, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=9, iceberg.field.optional=true}), Column(Name=af_exac, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=10, iceberg.field.optional=true}), Column(Name=clnvcso, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=11, iceberg.field.optional=true}), Column(Name=sciscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=12, iceberg.field.optional=true}), Column(Name=geneinfo, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=13, iceberg.field.optional=true}), Column(Name=clnsigincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=14, iceberg.field.optional=true}), Column(Name=oncdn, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=15, iceberg.field.optional=true}), Column(Name=clnvi, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=16, iceberg.field.optional=true}), Column(Name=clndisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=17, iceberg.field.optional=true}), Column(Name=sciincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=18, iceberg.field.optional=true}), Column(Name=clnrevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=19, iceberg.field.optional=true}), Column(Name=onc, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=20, iceberg.field.optional=true}), Column(Name=oncdnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=21, iceberg.field.optional=true}), Column(Name=alleleid, Type=int, Parameters={iceberg.field.current=true, iceberg.field.id=22, iceberg.field.optional=true}), Column(Name=scidisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=23, iceberg.field.optional=true}), Column(Name=origin, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=24, iceberg.field.optional=true}), Column(Name=scidisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=25, iceberg.field.optional=true}), Column(Name=clnsigscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=26, iceberg.field.optional=true}), Column(Name=clndnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=27, iceberg.field.optional=true}), Column(Name=oncscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=28, iceberg.field.optional=true}), Column(Name=scirevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=29, iceberg.field.optional=true}), Column(Name=sci, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=30, iceberg.field.optional=true}), Column(Name=rs, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=31, iceberg.field.optional=true}), Column(Name=dbvarid, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=32, iceberg.field.optional=true}), Column(Name=af_tgp, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=33, iceberg.field.optional=true}), Column(Name=oncdisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=34, iceberg.field.optional=true}), Column(Name=clnvc, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=35, iceberg.field.optional=true}), Column(Name=clnhgvs, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=36, iceberg.field.optional=true}), Column(Name=mc, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=37, iceberg.field.optional=true}), Column(Name=oncincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=38, iceberg.field.optional=true}), Column(Name=oncconf, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=39, iceberg.field.optional=true}), Column(Name=af_esp, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=40, iceberg.field.optional=true}), Column(Name=scidn, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=41, iceberg.field.optional=true}), Column(Name=clndisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=42, iceberg.field.optional=true}), Column(Name=scidnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=43, iceberg.field.optional=true}), Column(Name=oncdisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=44, iceberg.field.optional=true}), Column(Name=oncrevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=45, iceberg.field.optional=true}), Column(Name=conditions, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=46, iceberg.field.optional=true}), Column(Name=inheritance, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=47, iceberg.field.optional=true})], Location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar, AdditionalLocations=[], Compressed=false, NumberOfBuckets=0, SortColumns=[], StoredAsSubDirectories=false), TableType=EXTERNAL_TABLE, Parameters={metadata_location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00008-b174e538-7b51-4952-ac66-d712ddb7e949.metadata.json, previous_metadata_location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00007-7c5f80a5-341c-479b-85a7-039d6803b40b.metadata.json, table_type=ICEBERG}, CreatedBy=arn:aws:sts::418295705741:assumed-role/AmazonEMR-ExecutionRole-1777389601404/00g59743sbl50409,00g5bknbcgfq5o0b, IsRegisteredWithLakeFormation=false, CatalogId=418295705741, VersionId=8, IsMultiDialectView=false, IsMaterializedView=false) in catalog null with scanPlanningEnabled=false, dataCommitEnabled=false
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00008-b174e538-7b51-4952-ac66-d712ddb7e949.metadata.json
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO GlueCatalog: Table loaded by catalog: opendatalake.opendatalake_poc.clinvar
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO SparkTable: Table opendatalake.opendatalake_poc.clinvar loaded Spark schema: StructType(StructField(chromosome,StringType,true),StructField(start,LongType,true),StructField(end,LongType,true),StructField(reference,StringType,true),StructField(alternate,StringType,true),StructField(interpretations,ArrayType(StringType,true),true),StructField(name,StringType,true),StructField(clin_sig,ArrayType(StringType,true),true),StructField(clin_sig_conflict,ArrayType(StringType,true),true),StructField(af_exac,DoubleType,true),StructField(clnvcso,StringType,true),StructField(sciscv,ArrayType(StringType,true),true),StructField(geneinfo,StringType,true),StructField(clnsigincl,ArrayType(StringType,true),true),StructField(oncdn,ArrayType(StringType,true),true),StructField(clnvi,ArrayType(StringType,true),true),StructField(clndisdb,ArrayType(StringType,true),true),StructField(sciincl,ArrayType(StringType,true),true),StructField(clnrevstat,ArrayType(StringType,true),true),StructField(onc,ArrayType(StringType,true),true),StructField(oncdnincl,ArrayType(StringType,true),true),StructField(alleleid,IntegerType,true),StructField(scidisdbincl,ArrayType(StringType,true),true),StructField(origin,ArrayType(StringType,true),true),StructField(scidisdb,ArrayType(StringType,true),true),StructField(clnsigscv,ArrayType(StringType,true),true),StructField(clndnincl,ArrayType(StringType,true),true),StructField(oncscv,ArrayType(StringType,true),true),StructField(scirevstat,ArrayType(StringType,true),true),StructField(sci,ArrayType(StringType,true),true),StructField(rs,ArrayType(StringType,true),true),StructField(dbvarid,ArrayType(StringType,true),true),StructField(af_tgp,DoubleType,true),StructField(oncdisdb,ArrayType(StringType,true),true),StructField(clnvc,StringType,true),StructField(clnhgvs,ArrayType(StringType,true),true),StructField(mc,ArrayType(StringType,true),true),StructField(oncincl,ArrayType(StringType,true),true),StructField(oncconf,ArrayType(StringType,true),true),StructField(af_esp,DoubleType,true),StructField(scidn,ArrayType(StringType,true),true),StructField(clndisdbincl,ArrayType(StringType,true),true),StructField(scidnincl,ArrayType(StringType,true),true),StructField(oncdisdbincl,ArrayType(StringType,true),true),StructField(oncrevstat,ArrayType(StringType,true),true),StructField(conditions,ArrayType(StringType,true),true),StructField(inheritance,ArrayType(StringType,true),true))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO GlueCatalog: Table properties set at catalog level through catalog properties: {}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO GlueUtil: Not use extensions: table properties {owner=hadoop}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO GlueCatalog: Table properties enforced at catalog level through catalog properties: {}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00008-b174e538-7b51-4952-ac66-d712ddb7e949.metadata.json
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:27 INFO SparkTable: Table opendatalake_poc.clinvar loaded Spark schema: StructType(StructField(chromosome,StringType,true),StructField(start,LongType,true),StructField(end,LongType,true),StructField(reference,StringType,true),StructField(alternate,StringType,true),StructField(interpretations,ArrayType(StringType,true),true),StructField(name,StringType,true),StructField(clin_sig,ArrayType(StringType,true),true),StructField(clin_sig_conflict,ArrayType(StringType,true),true),StructField(af_exac,DoubleType,true),StructField(clnvcso,StringType,true),StructField(sciscv,ArrayType(StringType,true),true),StructField(geneinfo,StringType,true),StructField(clnsigincl,ArrayType(StringType,true),true),StructField(oncdn,ArrayType(StringType,true),true),StructField(clnvi,ArrayType(StringType,true),true),StructField(clndisdb,ArrayType(StringType,true),true),StructField(sciincl,ArrayType(StringType,true),true),StructField(clnrevstat,ArrayType(StringType,true),true),StructField(onc,ArrayType(StringType,true),true),StructField(oncdnincl,ArrayType(StringType,true),true),StructField(alleleid,IntegerType,true),StructField(scidisdbincl,ArrayType(StringType,true),true),StructField(origin,ArrayType(StringType,true),true),StructField(scidisdb,ArrayType(StringType,true),true),StructField(clnsigscv,ArrayType(StringType,true),true),StructField(clndnincl,ArrayType(StringType,true),true),StructField(oncscv,ArrayType(StringType,true),true),StructField(scirevstat,ArrayType(StringType,true),true),StructField(sci,ArrayType(StringType,true),true),StructField(rs,ArrayType(StringType,true),true),StructField(dbvarid,ArrayType(StringType,true),true),StructField(af_tgp,DoubleType,true),StructField(oncdisdb,ArrayType(StringType,true),true),StructField(clnvc,StringType,true),StructField(clnhgvs,ArrayType(StringType,true),true),StructField(mc,ArrayType(StringType,true),true),StructField(oncincl,ArrayType(StringType,true),true),StructField(oncconf,ArrayType(StringType,true),true),StructField(af_esp,DoubleType,true),StructField(scidn,ArrayType(StringType,true),true),StructField(clndisdbincl,ArrayType(StringType,true),true),StructField(scidnincl,ArrayType(StringType,true),true),StructField(oncdisdbincl,ArrayType(StringType,true),true),StructField(oncrevstat,ArrayType(StringType,true),true),StructField(conditions,ArrayType(StringType,true),true),StructField(inheritance,ArrayType(StringType,true),true))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO FileSourceStrategy: Pushed Filters: 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO FileSourceStrategy: Post-Scan Filters: (size(array_remove(array_union(split(regexp_replace(concat_ws(|, INFO_CLNSIG#30), ^_|\|_|/, |, 1), \|, -1), split(regexp_replace(concat_ws(|, INFO_CLNSIGCONF#42), \(\d{1,2}\), , 1), \|, -1)), ), true) > 0)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO VCFFileFormat: hlsUsage:[vcfRead,{"flattenInfoFields":true,"includeSampleIds":true,"useFilterParser":true,"useTabixIndex":true}]
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO GPLNativeCodeLoader: Loaded native gpl library
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev 049362b7cf53ff5f739d6b1532457f2c6cd495e8]
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO FileSourceScanExec: Planning scan with bin packing, max size: 16315616 bytes, open cost is considered as scanning 4194304 bytes, number of split files: 12, prefetch: false
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:28 INFO FileSourceScanExec: relation: None, fileSplitsInPartitionHistogram: Vector((1 fileSplits,12))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO CodeGenerator: Code generated in 454.26164 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO elasticshuffle/openshuffle: registering shuffle 5eab07e0-b965-4161-a843-f0244ae2b2f4 with 12 streams and 16 queues on aggregator [2600:1f18:3be7:8903:3892:618:5f2f:d284]:9200
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Registering RDD 10 (saveAsTable at GenericLoader.scala:38) as input to shuffle 1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Got map stage job 1 (saveAsTable at GenericLoader.scala:38) with 12 output partitions
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Final stage: ShuffleMapStage 2 (saveAsTable at GenericLoader.scala:38)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Parents of final stage: List()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Missing parents: List()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[10] at saveAsTable at GenericLoader.scala:38), which has no missing parents
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 299.2 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 95.5 KiB, actual size: 95.5 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 (size: 95.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1721
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO DAGScheduler: Submitting 12 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[10] at saveAsTable at GenericLoader.scala:38) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSchedulerImpl: Adding task set 2.0 with 12 tasks resource profile 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 24) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 0, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 1.0 in stage 2.0 (TID 25) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 1, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 2.0 in stage 2.0 (TID 26) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 2, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 3.0 in stage 2.0 (TID 27) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 3, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 4.0 in stage 2.0 (TID 28) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 4, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 5.0 in stage 2.0 (TID 29) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 5, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 6.0 in stage 2.0 (TID 30) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 6, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 7.0 in stage 2.0 (TID 31) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 7, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 8.0 in stage 2.0 (TID 32) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 8, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 9.0 in stage 2.0 (TID 33) ([2600:1f18:17c0:7100:2078:77fe:47d:901f], executor 1, partition 9, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 10.0 in stage 2.0 (TID 34) ([2600:1f18:17c0:7100:e040:979f:d721:132b], executor 2, partition 10, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO TaskSetManager: Starting task 11.0 in stage 2.0 (TID 35) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 11, ANY, 10118 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:29 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:02:54 INFO TaskSetManager: Finished task 11.0 in stage 2.0 (TID 35) in 24959 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (1/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:00 INFO TaskSetManager: Finished task 8.0 in stage 2.0 (TID 32) in 31284 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (2/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:01 INFO TaskSetManager: Finished task 2.0 in stage 2.0 (TID 26) in 31389 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (3/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:01 INFO TaskSetManager: Finished task 5.0 in stage 2.0 (TID 29) in 31898 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (4/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:03 INFO TaskSetManager: Finished task 9.0 in stage 2.0 (TID 33) in 33561 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (5/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:03 INFO TaskSetManager: Finished task 6.0 in stage 2.0 (TID 30) in 33851 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (6/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:03 INFO TaskSetManager: Finished task 3.0 in stage 2.0 (TID 27) in 33981 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (7/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:03 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 24) in 34006 ms on [2600:1f18:17c0:7100:2078:77fe:47d:901f] (executor 1) (8/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:03 INFO TaskSetManager: Finished task 1.0 in stage 2.0 (TID 25) in 34132 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (9/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSetManager: Finished task 7.0 in stage 2.0 (TID 31) in 34372 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (10/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSetManager: Finished task 4.0 in stage 2.0 (TID 28) in 34686 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (11/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSetManager: Finished task 10.0 in stage 2.0 (TID 34) in 34814 ms on [2600:1f18:17c0:7100:e040:979f:d721:132b] (executor 2) (12/12)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: ShuffleMapStage 2 (saveAsTable at GenericLoader.scala:38) finished in 34.916 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: looking for newly runnable stages
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: running: Set()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: waiting: Set()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: failed: Set()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO RedshiftStrategy: No redshift relations, skip push down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO ShufflePartitionsUtil: For shuffle(1), advisory target size: 67108864, actual target size 67108864, minimum partition size: 1048576
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO ShufflePartitionsUtil: For shuffle(1), advisory target size: 67108864, actual target size 67108864, minimum partition size: 1048576
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO CodeGenerator: Code generated in 84.443037 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 32.0 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 4.6 KiB, actual size: 4.6 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Removed broadcast_2_piece0 on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 in memory (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 (size: 4.6 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Removed broadcast_2_piece0 on [2600:1f18:17c0:7100:e040:979f:d721:132b]:39741 in memory (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Removed broadcast_2_piece0 on [2600:1f18:17c0:7100:2078:77fe:47d:901f]:35955 in memory (size: 95.5 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Removed broadcast_2_piece0 on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 in memory (size: 95.5 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO SparkContext: Created broadcast 3 from broadcast at SparkWrite.java:195
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO OverwriteByExpressionExec: Start processing data source write support: IcebergBatchWrite(table=opendatalake_poc.clinvar, format=PARQUET). The input RDD has 1 partitions.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO SparkContext: Starting job: saveAsTable at GenericLoader.scala:38
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Got job 2 (saveAsTable at GenericLoader.scala:38) with 1 output partitions
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Final stage: ResultStage 4 (saveAsTable at GenericLoader.scala:38)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 3)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Missing parents: List()
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Submitting ResultStage 4 (CoalescedRDD[14] at saveAsTable at GenericLoader.scala:38), which has no missing parents
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 335.4 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 110.6 KiB, actual size: 110.6 KiB, free 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on [2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:34121 (size: 110.6 KiB, free: 8.2 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:1721
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 4 (CoalescedRDD[14] at saveAsTable at GenericLoader.scala:38) (first 15 tasks are for partitions Vector(0))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSchedulerImpl: Adding task set 4.0 with 1 tasks resource profile 0
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 36) ([2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951], executor 3, partition 0, ANY, 10059 bytes) 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:04 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 (size: 110.6 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:05 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951]:33585 (size: 4.6 KiB, free: 7.9 GiB)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:03:29 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO EmrServerlessClusterSchedulerBackend: Requesting to kill executor(s) 1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO EmrServerlessClusterSchedulerBackend: Actual list of executor(s) to be killed is 1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO ExecutorContainerAllocator: Set total expected execs to {0=0}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO ExecutorAllocationManager: Executors 1 removed due to idle timeout.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO TaskSchedulerImpl: Executor 1 on [2600:1f18:17c0:7100:2078:77fe:47d:901f] killed by driver.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO DAGScheduler: Executor lost: 1 (epoch 2)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO ExecutorMonitor: Executor 1 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 1, unexpectedly exited: 0).
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO BlockManagerMasterEndpoint: Trying to remove executor 1 from BlockManagerMaster.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(1, [2600:1f18:17c0:7100:2078:77fe:47d:901f], 35955, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO BlockManagerMaster: Removed 1 successfully in removeExecutor
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:03 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f18:17c0:7100:2078:77fe:47d:901f:56440
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO EmrServerlessClusterSchedulerBackend: Requesting to kill executor(s) 2
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO EmrServerlessClusterSchedulerBackend: Actual list of executor(s) to be killed is 2
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO ExecutorContainerAllocator: Set total expected execs to {0=0}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO ExecutorAllocationManager: Executors 2 removed due to idle timeout.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO TaskSchedulerImpl: Executor 2 on [2600:1f18:17c0:7100:e040:979f:d721:132b] killed by driver.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO DAGScheduler: Executor lost: 2 (epoch 2)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO BlockManagerMasterEndpoint: Trying to remove executor 2 from BlockManagerMaster.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(2, [2600:1f18:17c0:7100:e040:979f:d721:132b], 39741, None)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO ExecutorMonitor: Executor 2 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 2, unexpectedly exited: 0).
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO BlockManagerMaster: Removed 2 successfully in removeExecutor
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:04 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f18:17c0:7100:e040:979f:d721:132b:57674
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:09 WARN DefaultEmrServerlessRMClient: Encountered errors when releasing containers: [ContainerError(ContainerGroupId=00cef824-b6fb-d431-a610-2b5e515cce5a, ContainerId=e0cef824-b7cf-0d81-f356-56f59498d6ae, ErrorCode=INTERNAL_ERROR)]
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:04:29 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:29 INFO MembrainClientProvider: Updated host list. Size: 54
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 36) in 145691 ms on [2600:1f18:17c0:7100:b3f9:6bf9:5a6c:1951] (executor 3) (1/1)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool 
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO DAGScheduler: ResultStage 4 (saveAsTable at GenericLoader.scala:38) finished in 145.747 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO TaskSchedulerImpl: Killing all running tasks in stage 4: Stage finished
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO DAGScheduler: Job 2 finished: saveAsTable at GenericLoader.scala:38, took 145.754399 s
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO OverwriteByExpressionExec: Data source write support IcebergBatchWrite(table=opendatalake_poc.clinvar, format=PARQUET) is committing.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO SparkWrite: Committing overwrite by filter true with 1 new data files to table opendatalake_poc.clinvar
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO SnapshotProducer: Committed snapshot 8067143916351935291 (BaseOverwriteFiles)
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:30 INFO LoggingMetricsReporter: Received metrics report: CommitReport{tableName=opendatalake_poc.clinvar, snapshotId=8067143916351935291, sequenceNumber=10, operation=overwrite, commitMetrics=CommitMetricsResult{totalDuration=TimerResult{timeUnit=NANOSECONDS, totalDuration=PT0.399889351S, count=1}, attempts=CounterResult{unit=COUNT, value=1}, addedDataFiles=CounterResult{unit=COUNT, value=1}, removedDataFiles=null, totalDataFiles=CounterResult{unit=COUNT, value=1}, addedDeleteFiles=null, addedEqualityDeleteFiles=null, addedPositionalDeleteFiles=null, addedDVs=null, removedDeleteFiles=null, removedEqualityDeleteFiles=null, removedPositionalDeleteFiles=null, removedDVs=null, totalDeleteFiles=CounterResult{unit=COUNT, value=0}, addedRecords=CounterResult{unit=COUNT, value=4185298}, removedRecords=null, totalRecords=CounterResult{unit=COUNT, value=4185298}, addedFilesSizeInBytes=CounterResult{unit=BYTES, value=165060724}, removedFilesSizeInBytes=null, totalFilesSizeInBytes=CounterResult{unit=BYTES, value=165060724}, addedPositionalDeletes=null, removedPositionalDeletes=null, totalPositionalDeletes=CounterResult{unit=COUNT, value=0}, addedEqualityDeletes=null, removedEqualityDeletes=null, totalEqualityDeletes=CounterResult{unit=COUNT, value=0}, manifestsCreated=null, manifestsReplaced=null, manifestsKept=null, manifestEntriesProcessed=null}, metadata={engine-version=3.5.6-amzn-1, app-id=00g5dhds4t4hvo0b, engine-name=spark, iceberg-version=Apache Iceberg unknown (commit unknown)}}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO SparkWrite: Committed in 431 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO OverwriteByExpressionExec: Data source write support IcebergBatchWrite(table=opendatalake_poc.clinvar, format=PARQUET) committed.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO SQLExecution: Generating and posting SparkListenerSQLExecutionObfuscatedInfo...
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO SQLExecution: Posted SparkListenerSQLExecutionObfuscatedInfo in 4 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO BaseMetastoreTableOperations: Successfully committed to table opendatalake.opendatalake_poc.clinvar in 304 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO SQLExecution: Generating and posting SparkListenerSQLExecutionObfuscatedInfo...
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO SQLExecution: Posted SparkListenerSQLExecutionObfuscatedInfo in 1 ms
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO Clinvar: Succeeded to load normalized_clinvar
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO HiveUtils: Initializing HiveMetastoreConnection version 2.3.9-amzn-4 using Spark classes.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO HiveClientImpl: Warehouse location for Hive client (version 2.3.9) is file:/home/hadoop/spark-warehouse
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO GlueUtils: Setting region to : us-east-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AWSGlueClientFactory: Setting glue service region to us-east-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO GlueUtils: Setting region to : us-east-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO GlueUtils: Setting region to : us-east-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:31 INFO AWSStsClientFactory: Setting sts service region to us-east-1
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO GlueUtil: Not use extensions: table Table(Name=clinvar, DatabaseName=opendatalake_poc, CreateTime=2026-05-01T15:30:37Z, UpdateTime=2026-05-04T00:05:31Z, Retention=0, StorageDescriptor=StorageDescriptor(Columns=[Column(Name=chromosome, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=1, iceberg.field.optional=true}), Column(Name=start, Type=bigint, Parameters={iceberg.field.current=true, iceberg.field.id=2, iceberg.field.optional=true}), Column(Name=end, Type=bigint, Parameters={iceberg.field.current=true, iceberg.field.id=3, iceberg.field.optional=true}), Column(Name=reference, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=4, iceberg.field.optional=true}), Column(Name=alternate, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=5, iceberg.field.optional=true}), Column(Name=interpretations, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=6, iceberg.field.optional=true}), Column(Name=name, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=7, iceberg.field.optional=true}), Column(Name=clin_sig, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=8, iceberg.field.optional=true}), Column(Name=clin_sig_conflict, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=9, iceberg.field.optional=true}), Column(Name=af_exac, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=10, iceberg.field.optional=true}), Column(Name=clnvcso, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=11, iceberg.field.optional=true}), Column(Name=sciscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=12, iceberg.field.optional=true}), Column(Name=geneinfo, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=13, iceberg.field.optional=true}), Column(Name=clnsigincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=14, iceberg.field.optional=true}), Column(Name=oncdn, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=15, iceberg.field.optional=true}), Column(Name=clnvi, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=16, iceberg.field.optional=true}), Column(Name=clndisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=17, iceberg.field.optional=true}), Column(Name=sciincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=18, iceberg.field.optional=true}), Column(Name=clnrevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=19, iceberg.field.optional=true}), Column(Name=onc, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=20, iceberg.field.optional=true}), Column(Name=oncdnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=21, iceberg.field.optional=true}), Column(Name=alleleid, Type=int, Parameters={iceberg.field.current=true, iceberg.field.id=22, iceberg.field.optional=true}), Column(Name=scidisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=23, iceberg.field.optional=true}), Column(Name=origin, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=24, iceberg.field.optional=true}), Column(Name=scidisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=25, iceberg.field.optional=true}), Column(Name=clnsigscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=26, iceberg.field.optional=true}), Column(Name=clndnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=27, iceberg.field.optional=true}), Column(Name=oncscv, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=28, iceberg.field.optional=true}), Column(Name=scirevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=29, iceberg.field.optional=true}), Column(Name=sci, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=30, iceberg.field.optional=true}), Column(Name=rs, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=31, iceberg.field.optional=true}), Column(Name=dbvarid, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=32, iceberg.field.optional=true}), Column(Name=af_tgp, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=33, iceberg.field.optional=true}), Column(Name=oncdisdb, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=34, iceberg.field.optional=true}), Column(Name=clnvc, Type=string, Parameters={iceberg.field.current=true, iceberg.field.id=35, iceberg.field.optional=true}), Column(Name=clnhgvs, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=36, iceberg.field.optional=true}), Column(Name=mc, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=37, iceberg.field.optional=true}), Column(Name=oncincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=38, iceberg.field.optional=true}), Column(Name=oncconf, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=39, iceberg.field.optional=true}), Column(Name=af_esp, Type=double, Parameters={iceberg.field.current=true, iceberg.field.id=40, iceberg.field.optional=true}), Column(Name=scidn, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=41, iceberg.field.optional=true}), Column(Name=clndisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=42, iceberg.field.optional=true}), Column(Name=scidnincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=43, iceberg.field.optional=true}), Column(Name=oncdisdbincl, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=44, iceberg.field.optional=true}), Column(Name=oncrevstat, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=45, iceberg.field.optional=true}), Column(Name=conditions, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=46, iceberg.field.optional=true}), Column(Name=inheritance, Type=array<string>, Parameters={iceberg.field.current=true, iceberg.field.id=47, iceberg.field.optional=true})], Location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar, AdditionalLocations=[], Compressed=false, NumberOfBuckets=0, SortColumns=[], StoredAsSubDirectories=false), TableType=EXTERNAL_TABLE, Parameters={metadata_location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00009-15949d46-55b0-47c9-b54e-c64698434059.metadata.json, previous_metadata_location=s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00008-b174e538-7b51-4952-ac66-d712ddb7e949.metadata.json, table_type=ICEBERG}, CreatedBy=arn:aws:sts::418295705741:assumed-role/AmazonEMR-ExecutionRole-1777389601404/00g59743sbl50409,00g5bknbcgfq5o0b, IsRegisteredWithLakeFormation=false, CatalogId=418295705741, VersionId=9, IsMultiDialectView=false, IsMaterializedView=false) in catalog null with scanPlanningEnabled=false, dataCommitEnabled=false
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO AuditContextUtil: Getting current Glue AuditContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO AuditContextUtil: Using AuditContext: {"platformType":"EMR_SERVERLESS","jobIdentifier":"00g59743sbl50409/00g5dhds4t4hvo0b"}
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: s3a://radiant-tst-datalake-qa/opendatalake/normalized/clinvar/metadata/00009-15949d46-55b0-47c9-b54e-c64698434059.metadata.json
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO GlueCatalog: Table loaded by catalog: opendatalake.opendatalake_poc.clinvar
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkTable: Table opendatalake.opendatalake_poc.clinvar loaded Spark schema: StructType(StructField(chromosome,StringType,true),StructField(start,LongType,true),StructField(end,LongType,true),StructField(reference,StringType,true),StructField(alternate,StringType,true),StructField(interpretations,ArrayType(StringType,true),true),StructField(name,StringType,true),StructField(clin_sig,ArrayType(StringType,true),true),StructField(clin_sig_conflict,ArrayType(StringType,true),true),StructField(af_exac,DoubleType,true),StructField(clnvcso,StringType,true),StructField(sciscv,ArrayType(StringType,true),true),StructField(geneinfo,StringType,true),StructField(clnsigincl,ArrayType(StringType,true),true),StructField(oncdn,ArrayType(StringType,true),true),StructField(clnvi,ArrayType(StringType,true),true),StructField(clndisdb,ArrayType(StringType,true),true),StructField(sciincl,ArrayType(StringType,true),true),StructField(clnrevstat,ArrayType(StringType,true),true),StructField(onc,ArrayType(StringType,true),true),StructField(oncdnincl,ArrayType(StringType,true),true),StructField(alleleid,IntegerType,true),StructField(scidisdbincl,ArrayType(StringType,true),true),StructField(origin,ArrayType(StringType,true),true),StructField(scidisdb,ArrayType(StringType,true),true),StructField(clnsigscv,ArrayType(StringType,true),true),StructField(clndnincl,ArrayType(StringType,true),true),StructField(oncscv,ArrayType(StringType,true),true),StructField(scirevstat,ArrayType(StringType,true),true),StructField(sci,ArrayType(StringType,true),true),StructField(rs,ArrayType(StringType,true),true),StructField(dbvarid,ArrayType(StringType,true),true),StructField(af_tgp,DoubleType,true),StructField(oncdisdb,ArrayType(StringType,true),true),StructField(clnvc,StringType,true),StructField(clnhgvs,ArrayType(StringType,true),true),StructField(mc,ArrayType(StringType,true),true),StructField(oncincl,ArrayType(StringType,true),true),StructField(oncconf,ArrayType(StringType,true),true),StructField(af_esp,DoubleType,true),StructField(scidn,ArrayType(StringType,true),true),StructField(clndisdbincl,ArrayType(StringType,true),true),StructField(scidnincl,ArrayType(StringType,true),true),StructField(oncdisdbincl,ArrayType(StringType,true),true),StructField(oncrevstat,ArrayType(StringType,true),true),StructField(conditions,ArrayType(StringType,true),true),StructField(inheritance,ArrayType(StringType,true),true))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkTable: Table opendatalake.opendatalake_poc.clinvar loaded Spark schema: StructType(StructField(chromosome,StringType,true),StructField(start,LongType,true),StructField(end,LongType,true),StructField(reference,StringType,true),StructField(alternate,StringType,true),StructField(interpretations,ArrayType(StringType,true),true),StructField(name,StringType,true),StructField(clin_sig,ArrayType(StringType,true),true),StructField(clin_sig_conflict,ArrayType(StringType,true),true),StructField(af_exac,DoubleType,true),StructField(clnvcso,StringType,true),StructField(sciscv,ArrayType(StringType,true),true),StructField(geneinfo,StringType,true),StructField(clnsigincl,ArrayType(StringType,true),true),StructField(oncdn,ArrayType(StringType,true),true),StructField(clnvi,ArrayType(StringType,true),true),StructField(clndisdb,ArrayType(StringType,true),true),StructField(sciincl,ArrayType(StringType,true),true),StructField(clnrevstat,ArrayType(StringType,true),true),StructField(onc,ArrayType(StringType,true),true),StructField(oncdnincl,ArrayType(StringType,true),true),StructField(alleleid,IntegerType,true),StructField(scidisdbincl,ArrayType(StringType,true),true),StructField(origin,ArrayType(StringType,true),true),StructField(scidisdb,ArrayType(StringType,true),true),StructField(clnsigscv,ArrayType(StringType,true),true),StructField(clndnincl,ArrayType(StringType,true),true),StructField(oncscv,ArrayType(StringType,true),true),StructField(scirevstat,ArrayType(StringType,true),true),StructField(sci,ArrayType(StringType,true),true),StructField(rs,ArrayType(StringType,true),true),StructField(dbvarid,ArrayType(StringType,true),true),StructField(af_tgp,DoubleType,true),StructField(oncdisdb,ArrayType(StringType,true),true),StructField(clnvc,StringType,true),StructField(clnhgvs,ArrayType(StringType,true),true),StructField(mc,ArrayType(StringType,true),true),StructField(oncincl,ArrayType(StringType,true),true),StructField(oncconf,ArrayType(StringType,true),true),StructField(af_esp,DoubleType,true),StructField(scidn,ArrayType(StringType,true),true),StructField(clndisdbincl,ArrayType(StringType,true),true),StructField(scidnincl,ArrayType(StringType,true),true),StructField(oncdisdbincl,ArrayType(StringType,true),true),StructField(oncrevstat,ArrayType(StringType,true),true),StructField(conditions,ArrayType(StringType,true),true),StructField(inheritance,ArrayType(StringType,true),true))
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkContext: Invoking stop() from shutdown hook
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkContext: SparkContext is stopping with exitCode 0.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkUI: Stopped Spark web UI at http://[2600:1f18:17c0:7100:dac6:b3e9:1cf7:bffc]:4040
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO EmrServerlessClusterSchedulerBackend: Shutting down all executors
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Asking each executor to shut down
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO TimeBasedRotatingEventLogFilesWriter: Renaming file:/var/log/spark/apps/eventlog_v2_00g5dhds4t4hvo0b/00g5dhds4t4hvo0b.inprogress to file:/var/log/spark/apps/eventlog_v2_00g5dhds4t4hvo0b/events_1_00g5dhds4t4hvo0b
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO MemoryStore: MemoryStore cleared
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO BlockManager: BlockManager stopped
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO BlockManagerMaster: BlockManagerMaster stopped
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO SparkContext: Successfully stopped SparkContext
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO ShutdownHookManager: Shutdown hook called
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO ShutdownHookManager: Deleting directory /tmp/membrain/spark-8b719dad-570f-4074-ad7e-a63093ef63dd
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO ShutdownHookManager: Deleting directory /tmp/spark-119fc895-dc94-4271-9e3f-cddee36f0b75
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO MetricsSystemImpl: Stopping s3a-file-system metrics system...
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO MetricsSystemImpl: s3a-file-system metrics system stopped.
[2026-05-04, 00:06:07 UTC] {emr.py:86} INFO - 26/05/04 00:05:32 INFO MetricsSystemImpl: s3a-file-system metrics system shutdown complete.
[2026-05-04, 00:06:07 UTC] {taskinstance.py:340} ▶ Post task execution logs
```