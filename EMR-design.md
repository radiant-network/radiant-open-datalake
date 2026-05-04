# EMR Deployment Design — radiant-open-datalake Spark Jobs

**Status:** Draft · **Date:** 2026-04-22

## 1. Context

The `spark/` module packages ~26 Scala ETL jobs (see `ImportPublicTable.scala`)
that normalize raw public genomic sources (ClinVar, dbSNP, gnomAD v3/v4 joint/CNV/SV,
dbNSFP, TopMed Bravo, 1000Genomes, SpliceAI, Orphanet, OMIM, HPO, RefSeq, COSMIC, DDD)
from `s3://opendatalake-<env>/raw/landing/` into Iceberg tables at
`s3://opendatalake-<env>/iceberg/reference/`.

Jobs are triggered by Airflow (MWAA, per `config.py` references to MWAA startup
scripts). Current Airflow operators wrap ECS Fargate (`ecs.py`) and K8s
(`k8s.py`) for **Python** tasks only. **No Spark execution runtime exists yet** —
this doc selects one.

### Key constraints

| Constraint | Value |
|---|---|
| Spark version | 3.5.5 (build.sbt) |
| Scala | 2.12.18 |
| Iceberg | 1.10.1 |
| Extra runtime deps | Glow 2.0.0 (genomics), hadoop-aws 3.3.4, iceberg-aws-bundle |
| Catalog | Iceberg REST (per `storage_convention.md` — Polaris-style root) |
| Trigger cadence | Daily/weekly, post-download (Airflow DAG) |
| Workload shape | Bursty, highly heterogeneous (gnomAD joint v4 ≈ TB; gene sets ≈ MB) |
| Artifact | Fat JAR via `sbt assembly` → `radiant-open-datalake-spark.jar` |
| Existing Airflow | `apache-airflow-providers-amazon` 9.12.0 (ships EMR/EMR-Serverless operators) |

### Workload characterization (sizing matters)

| Tier | Datasets | Rough input | Exec sizing |
|---|---|---|---|
| XL | gnomAD v4 joint, gnomAD v3 genomes | 100s GB–TB VCF/bgz | Many cores, >128 GB mem, Glow split-multiallelics |
| L | dbNSFP, SpliceAI (snv/indel), TopMed Bravo, 1000Genomes | 10s–100s GB | Mid cluster |
| S | ClinVar, dbSNP, OMIM, HPO, Orphanet, RefSeq, COSMIC, DDD, gnomAD constraint | MB–low GB | 2–4 executors |

Tiering matters: a single fixed cluster wastes money on S-tier, throttles XL-tier.

---

## 2. Options

### A. EMR Serverless (**recommended**)

Per-job Spark application on an auto-scaled pool. Pay per vCPU-second and
GB-second while workers run; no cluster to manage.

**Pros**
- Per-job right-sizing — S-tier uses tiny workers; XL-tier scales out. Matches our bimodal workload without cluster tuning.
- Pre-initialized capacity (`initialCapacity`) drops cold-start to seconds → hot path for chained DAG tasks.
- Native Iceberg + Glue/REST catalog support in EMR 7.x releases (Spark 3.5.x available since EMR 7.1).
  Ref: [EMR Serverless release notes](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-serverless-release-versions.html).
- Airflow operator `EmrServerlessStartJobOperator` lives in `apache-airflow-providers-amazon` (already installed).
  Ref: [operator docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr/emr_serverless.html).
- Custom image supports injecting Glow + `iceberg-aws-bundle` + `hadoop-aws` cleanly (Glow native libs / shaded shapeless rules in build.sbt must be preserved).
  Ref: [EMR Serverless custom images](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html).
- VPC networking + IAM role per application → tight S3 bucket scoping.

**Cons**
- Per vCPU-hour ≈ 20–30% more than Spot EC2 at sustained >70% utilization. Our workload is bursty → not applicable.
- No direct shell/debug on running worker; must rely on driver logs in CloudWatch/S3.
- Executor memory-per-core ratios are coarser than on EC2 (fixed worker shapes).
- Max worker count is soft-limited per account; XL-tier (gnomAD joint) should be validated against limits.

### B. EMR on EC2 — transient cluster per job (or per DAG run)

`EmrCreateJobFlowOperator` + `EmrAddStepsOperator` + `EmrTerminateJobFlowOperator`.

**Pros**
- Cheapest at sustained load (Spot instances, reserved core).
- Full control of bootstrap actions, instance types, native lib placement (Glow).
- Mature, battle-tested for genomics pipelines.

**Cons**
- 5–8 min cluster bootstrap × N jobs × daily = wasted wall-clock + cost.
- Amortize by batching many Spark steps on one transient cluster → couples DAG topology to cluster lifecycle (ugly with Airflow asset-based scheduling already in use here).
- Ops burden: AMIs, bootstrap scripts, version upgrades.
- Spot interruptions on XL-tier cause retries from scratch — painful for multi-hour gnomAD joints.

### C. EMR on EKS

Spark submitted as `SparkApplication` CR on an existing EKS cluster; EMR provides runtime image + Glue catalog integration.
  Ref: [EMR on EKS](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html).

**Pros**
- Synergy with existing K8s-based Airflow operator (`k8s.py`) — one platform.
- Spark 3.5 / Iceberg 1.10 supported in recent EMR on EKS releases.
- Cluster-wide autoscaling (Karpenter) → elastic and cheap.

**Cons**
- Requires a production EKS cluster; none referenced in this repo today. New infra commitment.
- More moving parts (Karpenter, namespaces, RBAC, service accounts/IRSA) vs Serverless.
- Spark driver pod scheduling latency higher than Serverless pre-init capacity.

### D. AWS Glue

**Reject.** Glue 4.0 pins Spark 3.3; Glue 5.0 ships Spark 3.5.4 but has constrained worker shapes and historically fragile support for non-Glue-shipped JARs with native libs (Glow JNI). Also weaker Iceberg-REST vs Glue-catalog story.
  Ref: [Glue version matrix](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html).

### E. Self-managed Spark-on-Kubernetes (spark-operator)

**Reject.** All the ops cost of (C) with none of the managed runtime, no Glue/EMR catalog integrations, no vendor support. Only attractive if a non-AWS path is required.

---

## 3. Recommendation — EMR Serverless

**Primary runtime:** EMR Serverless (EMR release `emr-7.x` track, Spark 3.5.x).
**Escape hatch:** EMR on EC2 transient clusters via the same Airflow provider if we hit a Serverless blocker (worker-count ceiling on gnomAD joint v4; Glow native-lib issue).

### Why this fit

- Shape match: bursty, heterogeneous, batch, S3-anchored, Iceberg-centric.
- Zero cluster ops; team stays focused on ETL logic.
- Per-job IAM/VPC boundary is cleaner than shared EC2 cluster.
- Airflow integration is an operator swap in, not a platform project.

### Architecture

```
Airflow (MWAA) DAG: opendatalake-ingest-<source>
  ├─ [existing] download tasks (ECS Fargate)
  └─ [new] EmrServerlessStartJobOperator(
           application_id=<env-specific app>,
           execution_role_arn=<role>,
           job_driver={sparkSubmit: {
             entryPoint: s3://<bucket>/artifacts/radiant-open-datalake-spark.jar,
             entryPointArguments: [<dataset-name>, --config s3://.../<env>.conf],
             sparkSubmitParameters: "--class org.radiant.opendatalake.ImportPublicTable ..."
           }},
           configuration_overrides={
             monitoringConfiguration: {
               s3MonitoringConfiguration: {logUri: s3://<bucket>/emr-logs/}
             }
           })
         → Iceberg tables at s3://opendatalake-<env>/iceberg/reference/
```

### Sizing (starting point, tune from metrics)

| Tier | `initialCapacity` | Max workers | Driver | Executor |
|---|---|---|---|---|
| S | none (cold-start OK) | 10 | 2 vCPU / 8 GB | 2 vCPU / 8 GB |
| L | 2 executors warm | 40 | 4 vCPU / 16 GB | 4 vCPU / 30 GB |
| XL (gnomAD) | 5 executors warm | 100 | 8 vCPU / 32 GB | 8 vCPU / 60 GB |

Tier encoded per-`@main` entry in `ImportPublicTable.scala` via an Airflow-side
mapping (dict keyed by dataset name); cheaper than one Serverless app per tier.
Single app can accept heterogeneous jobs — `maximumCapacity` caps the app,
per-job `sparkSubmitParameters` set executor count.

### Custom image contents

Base: `public.ecr.aws/emr-serverless/spark/emr-7.x:latest`.
Add:
- `radiant-open-datalake-spark.jar` (our assembly) — or pass via `--jars` from S3 (preferred; avoids image rebuilds per release).
- `glow-spark3-2.0.0.jar` + transitive native libs (if not already inside the assembly shade).
- No hadoop-aws / iceberg-aws-bundle override — EMR runtime ships them.
  Check `build.sbt` `Provided` scopes still hold on EMR runtime.

### Deployment

- Terraform: one `aws_emrserverless_application` per env (`qa`, `staging`, `prod`) under `aws-infra-d3b-accounts`.
- Execution role: S3 RW on `opendatalake-<env>`, Glue `GetTable/CreateTable` if Glue catalog fallback, KMS decrypt.
- Artifact bucket: `s3://opendatalake-<env>-artifacts/spark/<git-sha>/radiant-open-datalake-spark.jar`. CI publishes on tag.
- Airflow var `emr_serverless_application_id_<env>` injected via MWAA startup (same pattern as existing ECS vars in `config.py`).

### Observability

- Driver/executor logs → S3 (`s3://.../emr-logs/`) + CloudWatch.
- Spark UI → EMR Serverless persistent UI endpoint (built-in, per application).
- Airflow task surfaces EMR job run URL via `EmrServerlessJobSensor` callback.

---

## 4. Rollout Plan

1. **PoC (1 week):** provision one `dev` EMR Serverless app. Run `clinvar` (smallest, fastest iteration). Validate Iceberg write to REST catalog + Glow deps.
2. **Tier validation (1 week):** run `dbnsfp` (L-tier) and `gnomadv4` (XL-tier). Capture vCPU-s / GB-s metrics; right-size `maximumCapacity`.
3. **Airflow operator wrapper:** add `dags/lib/operators/emr_serverless.py` mirroring the ECS/K8s pattern so DAGs stay short.
4. **Migrate source-by-source:** one DAG at a time, starting with S-tier. Keep a feature flag (Airflow Variable) to toggle EMR-Serverless vs local spark-submit for fallback.
5. **Decommission** any manual spark-submit / sandbox path after all sources migrated.

---

## 5. Open Questions / Risks

- **Glow native libs on EMR Serverless custom image.** Must be validated end-to-end with `flattenInfoFields` + `split_multiallelics` read options (used heavily in `EtlConfiguration.scala`).
- **Iceberg REST catalog endpoint reachability** from EMR Serverless VPC. Needs route/endpoint in Terraform.
- **gnomAD joint v4 ceiling:** TB-scale shuffle. May need Spark adaptive shuffle + S3A committer tuning; confirm EMR runtime defaults (magic committer on by default in EMR 7.x).
  Ref: [EMR S3A magic committer](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html).
- **Cost alerting:** set CloudWatch budget alarm per env; Serverless cost is easy to overrun during tuning.
- **Spot support:** EMR Serverless does not expose Spot directly. If cost becomes a concern for XL-tier, re-evaluate EMR on EC2 transient clusters only for those specific jobs (hybrid).

---

## Appendix A — Execution Flow: gnomAD v4 Joint (Recommended Option)

End-to-end component + dataflow trace for the XL-tier case, starting at the
Airflow operator and ending at the Iceberg catalog commit.

```
                    gnomAD v4 joint — execution flow
                    ===============================

┌─────────────────────────────────────────────────────────────────────────────┐
│                          AIRFLOW (MWAA env)                                 │
│                                                                             │
│   DAG: opendatalake-ingest-gnomadv4                                         │
│   ┌──────────────────────┐   ┌──────────────────────────────────────────┐   │
│   │ download_source      │──▶│ spark_gnomadv4  (EmrServerlessStart-     │   │
│   │  (ECS Fargate task)  │   │                  JobOperator)            │   │
│   └──────────────────────┘   └────────────────────┬─────────────────────┘   │
│                                                   │                         │
│   [Airflow worker] apache-airflow-providers-      │                         │
│   amazon 9.12.0 → EmrServerlessHook →             │                         │
│   boto3 emr-serverless client                     │                         │
└───────────────────────────────────────────────────┼─────────────────────────┘
                                                    │ StartJobRun API
                                                    │ (applicationId,
                                                    │  executionRoleArn,
                                                    │  sparkSubmitParameters,
                                                    │  entryPointArguments)
                                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      EMR SERVERLESS CONTROL PLANE                           │
│                                                                             │
│   Application: opendatalake-spark-prod (emr-7.x, Spark 3.5.x)               │
│   ┌───────────────────────────────────────────────────────────────┐         │
│   │ Job Run (jobRunId=...)                                        │         │
│   │   scheduler → allocate driver + executors from                │         │
│   │   initialCapacity pool (5 warm) + scale up to                 │         │
│   │   maximumCapacity (≤100 executors)                            │         │
│   └───────────────────────────────────────────────────────────────┘         │
│   IAM: assume execution-role → S3 RW + KMS decrypt                          │
│   VPC: customer subnets + SG (access S3 VPCE, Iceberg REST, KMS)            │
└───────────────────────────────────────────────────┬─────────────────────────┘
                                                    │ places pods on
                                                    │ Firecracker microVMs
                                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    EMR SERVERLESS DATA PLANE (per-job)                      │
│                                                                             │
│   ┌────────────────────────────────────┐                                    │
│   │  Spark DRIVER  (8 vCPU / 32 GB)    │                                    │
│   │  - SparkSession + IcebergCatalog   │                                    │
│   │  - DAG scheduler                   │                                    │
│   │  - fetches entryPoint JAR from S3  │                                    │
│   │    artifacts/radiant-open-         │                                    │
│   │    datalake-spark.jar              │                                    │
│   │  - class: o.r.opendatalake.        │                                    │
│   │    ImportPublicTable → main(       │                                    │
│   │    "gnomadv4", ...) → GnomadV4.run │                                    │
│   └──────────────┬─────────────────────┘                                    │
│                  │ broadcast plan + task sets                               │
│       ┌──────────┼──────────┬──────────┬──────────┐                         │
│       ▼          ▼          ▼          ▼          ▼                         │
│   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐                      │
│   │EXE 1 │   │EXE 2 │   │EXE 3 │   │ ...  │   │EXE N │  (autoscale)         │
│   │8c/60G│   │8c/60G│   │8c/60G│   │      │   │8c/60G│                      │
│   │+Glow │   │+Glow │   │+Glow │   │      │   │+Glow │  native .so libs     │
│   └──┬───┘   └──┬───┘   └──┬───┘   └──┬───┘   └──┬───┘                      │
└──────┼──────────┼──────────┼──────────┼──────────┼──────────────────────────┘
       │          │          │          │          │
       │ READ     │ READ     │ READ     │ READ     │ READ   (parallel,
       │ (split   │          │          │          │        partitioned
       │  by file)│          │          │          │        by chr)
       ▼          ▼          ▼          ▼          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               S3 (raw)                                      │
│  s3://opendatalake-prod/raw/landing/gnomad_v4/release/4.1/vcf/joint/        │
│    gnomad.joint.v4.1.sites.chr1.vcf.bgz                                     │
│    gnomad.joint.v4.1.sites.chr2.vcf.bgz                                     │
│    ... (chr[^M]* — 24 files, ~TB total bgzipped)                            │
└─────────────────────────────────────────────────────────────────────────────┘
       │          │          │          │          │
       │  Glow VCF reader:                                                    
       │    flattenInfoFields=true                                            
       │    split_multiallelics=true                                          
       │  → DataFrame[chr, pos, ref, alt, INFO.*]                             
       │                                                                      
       │  GnomadV4.run transforms:                                             
       │    select/rename, dedup, compute derived cols                        
       │                                                                      
       │  Shuffle (magic committer default on EMR 7.x)                        
       │                                                                      
       ▼
  writeTo("opendatalake.reference.gnomad_joint_v4").overwritePartitions()
       │
       ├────────── WRITE Parquet data files (parallel, per-executor) ────────┐
       │                                                                     │
       │                                                                     ▼
       │                         ┌──────────────────────────────────────────────┐
       │                         │                 S3 (iceberg)                 │
       │                         │  s3://opendatalake-prod/iceberg/reference/   │
       │                         │    gnomad_joint_v4/                          │
       │                         │      data/*.parquet                          │
       │                         │      metadata/                               │
       │                         │        snap-<id>-*.avro  (manifests)         │
       │                         │        <id>.metadata.json                    │
       │                         └──────────────────────────────────────────────┘
       │                                                                     ▲
       │ atomic commit ──────────────────────────────────────────────────────┘
       ▼                                (catalog updates current snapshot pointer)
┌─────────────────────────────────────┐
│      Iceberg REST Catalog           │
│  (Polaris / Tabular / Nessie /       │
│   Snowflake Open Catalog)            │
│  - namespace: opendatalake.reference │
│  - table: gnomad_joint_v4            │
│  - commit: CAS on metadata pointer   │
└─────────────────────────────────────┘
       ▲
       │ heartbeat / status poll
       │
┌──────┴──────────────────────────────┐
│  EmrServerlessJobSensor (Airflow)   │──▶ task SUCCESS/FAILURE
│  polls GetJobRun every N seconds    │    → downstream Airflow assets
│  streams logs to CloudWatch + S3    │
│    s3://.../emr-logs/<app>/<job>/   │
└─────────────────────────────────────┘
```

### Component count (Airflow op → worker): 7 layers

1. Airflow task → `EmrServerlessStartJobOperator`
2. `EmrServerlessHook` → boto3 `emr-serverless` client
3. EMR Serverless control plane (scheduler, IAM assume-role, VPC wiring)
4. EMR Serverless application (capacity pool, release image)
5. Job Run (driver + executor allocation on Firecracker microVMs)
6. Spark driver (JVM, SparkSession, Iceberg catalog client)
7. Spark executors N× (Glow JNI, S3A reader, Iceberg writer)

### Data path

S3 raw (bgzipped VCF, chr-sharded) → Glow reader per executor → transforms →
Parquet writer → S3 `iceberg/data/` → manifest Avro → REST catalog CAS commit
on `metadata.json`.

### Parallelism

- Input parallelism = file count (≈24 chr files); Glow can further split bgz via virtual offsets.
- Shuffle width tunable via `spark.sql.shuffle.partitions`.
- Executor count scales between `initialCapacity` (warm) and `maximumCapacity` (ceiling).

---

## 6. Alternatives Summary

| Option | Verdict | Primary reason |
|---|---|---|
| EMR Serverless | **Chosen** | Shape match + zero ops + existing Airflow provider |
| EMR on EC2 transient | Fallback | Cheaper at scale; keep as escape hatch for XL-tier |
| EMR on EKS | Defer | Needs EKS platform investment we don't have yet |
| AWS Glue | Reject | Spark/Glow version + JNI friction |
| Self-managed Spark/K8s | Reject | All the ops, none of the managed runtime |
