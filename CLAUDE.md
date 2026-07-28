# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**radiant-open-datalake** is a monorepo with two independent components:
- **`airflow/`** — Python orchestration for discovering and ingesting public genomic datasets
- **`spark/`** — Scala transformations that normalize/enrich raw data into Iceberg tables on S3

### Data Flow

```
External Sources (ClinVar, gnomAD, etc.)
    → Airflow DAG: discover-new-source-versions (daily 6AM UTC)
        → S3 raw landing: s3://opendatalake-<ENV>/raw/landing/<SOURCE>/<VERSION>/
    → Spark ETL jobs (triggered by DAG)
        → Iceberg tables: s3://opendatalake-<ENV>/iceberg/reference/
            → Iceberg catalog: opendatalake.reference.<table>
```

The contract between the two halves is the EMR Serverless invocation: `EmrServerlessJobOperator`
launches the Spark fat JAR with entry class `org.radiant.opendatalake.ImportPublicTable` and args

```
<command> --config config/<env>.conf --steps default [--version <v>] [--raw-storage <s3a-root>]
```

where `<command>` is a `@main` method name in `ImportPublicTable.scala` (see
`airflow/opendatalake/dags/import_source.py`).

---

# Airflow (Python)

## Commands

All commands run from the `airflow/` directory.

```bash
make install       # Install production deps with pinned constraints
make install-dev   # Install dev deps + initialize Airflow DB (run once for local setup)
make test          # Run lint + unit tests (what CI runs)
make test-static   # Ruff lint only
make test-unit     # Pytest unit tests only
make format        # Auto-format and fix lint issues with ruff
```

Run a single test file:
```bash
cd airflow && python -m pytest tests/unit/dags/test_discover_new_source_versions.py -v
```

Run a single test by name:
```bash
cd airflow && python -m pytest -k "test_function_name" -v
```

Local setup gotchas: use Python 3.12 (matches AWS), and set `export AIRFLOW_HOME=$(pwd)/.airflow_home` before `make install-dev` (which runs `airflow db reset`). A local sandbox for running DAGs is in `airflow/sandbox/` (Kubernetes-based) — it cannot run the AWS operators (ECS/EMR).

## Package Layout

The Python package root is `airflow/opendatalake/`: DAG files live in `opendatalake/dags/`, shared
code in `opendatalake/lib/`. Everything imports absolutely with the `opendatalake.` prefix
(`from opendatalake.lib...`). `pytest.ini` puts `.` on `pythonpath` so the package resolves from the
`airflow/` dir; on MWAA the package is synced under the DAGs folder (`s3://<bucket>/dags/opendatalake/`)
so it imports the same way. Most directories are implicit namespace packages (no `__init__.py`) —
only `opendatalake/lib/operators/` and `opendatalake/lib/domain/source_configs/` carry an `__init__.py`
to re-export their public symbols.

## DAG Structure

The main DAG (`discover_new_source_versions.py`) uses Airflow asset-based dependencies. Sources are defined declaratively:
- **`SourceConfig`** — describes a dataset source (URL, versioning strategy, update mode)
- **`DownloadConfig`** — describes download parameters
- **`_Source` enum** (`sources.py`) — registry of all configured sources; add new sources here

S3 transfers use multipart upload with resume support (`opendatalake/lib/s3_transfer.py`). The Airflow connection ID for S3 is `aws_default`; bucket name comes from the `environment` Airflow variable.

## Custom Operators & Configuration

Custom operators live in `opendatalake/lib/operators/` and are re-exported from its `__init__.py` (the discoverable list). Import them as `from opendatalake.lib.operators import PythonScriptOperator, EmrServerlessJobOperator`.

- **`PythonScriptOperator`** (`ecs.py`) — runs a Python download script on ECS Fargate.
- **`EmrServerlessJobOperator`** (`emr.py`) — launches the Spark fat JAR on EMR Serverless. Deferrable by default; forwards SPARK_DRIVER logs to the Airflow task log; builds the Iceberg/Glue Spark conf from config.

**Colocated env-var config pattern** (introduced SJRA-1570 — prefer this over Airflow Variables for new operators): each operator owns a frozen `@dataclass` config (`EcsConfig`, `EmrServerlessConfig`) with a module-level `_REQUIRED_ENV_VARS` map, a `@lru_cache`'d `from_env()` reading `os.getenv`, and `missing_required()`. Infra config is injected via `OPENDATALAKE_*` environment variables (set at deploy time, e.g. Terraform), validated at construction (raises `AirflowException` listing missing vars). Tests inject a config object directly and seed env defaults in `tests/unit/conftest.py` from each operator's `_REQUIRED_ENV_VARS`.

## Python Stack

- Python 3.12 (version-pinned; critical for AWS deployment compatibility), Airflow 3.x
- Exact versions are locked in `airflow/constraints-python3.12.txt` — read it rather than restating pins
- Linter: Ruff (line length 119, rules: E, F, UP, B, SIM, I) — config at `airflow/.ruff.toml`
- Tests: pytest, config at `airflow/pytest.ini` (`pythonpath = .` makes the `opendatalake` package importable from the `airflow/` dir)

---

# Spark (Scala)

## Commands

All commands run from the `spark/` directory.

```bash
sbt clean test                                                    # all tests (what CI runs)
sbt "testOnly *ClinvarSpec"                                       # single spec
sbt "testOnly *ClinvarSpec -- -z \"overwrite data\""              # single test by name
sbt assembly                                                      # fat JAR -> target/scala-2.12/radiant-open-datalake-spark.jar
sbt "runMain org.radiant.opendatalake.config.EtlConfiguration"    # regenerate resources/config/*.conf
```

Local end-to-end runs (MinIO + Iceberg REST catalog via Docker Compose, plus a working
`spark-submit` example): `spark/sandbox/README.md`. Storage/naming conventions:
`spark/doc/storage_convention.md`.

## Three layers, one config registry

```
raw_*        (S3 landing: VCF / CSV / GFF / XML, read-only)
  → normalized_*  (one Iceberg table per source, schema standardized)
    → enriched_*  (joins/aggregates across normalized tables)
```

`config/EtlConfiguration.scala` is the single registry of every dataset (`DatasetConf`) and storage
root (`StorageConf`). It is an `App`: running it **generates** `src/main/resources/config/prd.conf`
and `src/test/resources/config/test.conf`. Never hand-edit those `.conf` files — edit
`EtlConfiguration.scala` and regenerate, or the next `runMain` silently reverts the change.

Two storage ids only: `raw_storage` (`s3a://opendatalake-<env>/raw/landing`) and `iceberg_storage`
(`s3a://opendatalake-<env>/iceberg/reference`). Catalog is `opendatalake`, database `reference`, so
tables resolve as `opendatalake.reference.<table>`.

`test.conf` deliberately rewrites every Iceberg dataset path to `/<table-name>`: the local Hadoop
catalog used in tests rejects custom table locations. Keep that mapping intact when touching
`EtlConfiguration`.

## Job shape

Every job is a `case class Xxx(rc: RuntimeETLContext)` extending a FerLab base
(`bio.ferlab.datalake.spark3.etl.v4`) and paired with a companion `object` exposing a `@main run` —
except the contract-managed ones (`Clinvar`, `DBSNP`), which have no companion at all: a second
`@main` would be a launch path that skips contract selection and the destination check, so their only
entry point is `ImportPublicTable` dispatching through `ContractRunner`.

- `SimpleETLP` — normalized jobs (20 of them); publishes/partitions per the `DatasetConf`.
- `SimpleSingleETL` — enriched jobs and `DBNSFPRaw` (5).

Override `mainDestination` (a `conf.getDataset(...)` id), `extract`, `transformSingle`, and optionally
`defaultRepartition`. Genomic column helpers (`chromosome`, `start`, `locus`, `flattenInfo`,
`groupByLocus`, `vcf`) come from `GenomicImplicits`; VCF parsing is Glow.

`ImportPublicTable.scala` is the mainargs command registry and the JAR's entry class. Every dataset
needs an `@main` method there — the method name *is* the CLI subcommand Airflow passes
(`@main(name = "1000genomes")` renames it).

## Runtime-injected raw storage and versioned paths

Version-pinned sources (`clinvar`, `dbsnp`) take `Version` and `RawStorage` mainargs wrappers
(`mainutils/`) and read through `RawInput.readVersioned`, which substitutes `{{VERSION}}` into the
dataset path and swaps the raw `StorageConf` root for the `--raw-storage` value. That is why raw S3
roots are *not* authoritative in the generated conf for those sources — Airflow supplies the bucket
and the discovered version at launch time.

## Data contracts (`config/contracts/`, SJRA-1546 / SJRA-1747)

`spark/src/main/resources/contracts.yml` is the declared source of truth for which contracts the ETL
must execute: per source, a `lineage` `"{MAJOR}.{MINOR}"`, its `table`, the normalizer FQCN, and a
`release_notes` path (relative to `spark/`, under `spark/doc/release-notes/<source>/v<MAJOR>.md`).
MAJOR identifies the table/normalizer pair (new MAJOR = new row); MINOR is bumped in place when
columns are added by schema evolution. `Contracts.load()` parses it off the classpath with Jackson
YAML (snake_case → camelCase, `FAIL_ON_UNKNOWN_PROPERTIES` on, so a typo'd key throws). Jackson passes
*null*, never an empty collection, for empty yaml keys, so `Contracts` normalizes both a null
`sources` map and a null per-source value — otherwise a half-written file NPEs instead of reporting
itself.

`ContractsSpec` is the guard on the two untyped string fields: every `normalizer` must resolve to a
concrete `ETL` taking a `RuntimeETLContext`, and every `release_notes` path must exist on disk.

**Fan-out.** Contract-declared sources dispatch through `ContractRunner.run(source, …)` instead of a
hard-coded job: the CLI command names the *source*, `contracts.yml` decides which normalizers run for
it (SJRA-1546 §3.2). Adding a MAJOR = a `contracts.yml` row + a `ContractRegistry` entry, no CLI
change. `ContractRegistry` maps the declared FQCN to a factory (keys are `classOf[…].getName`, so
renames follow) — a registry rather than reflection because normalizer constructor arities differ.
The fan-out builds and validates every job before running any (`ContractRunner.build`): unknown
source, a MAJOR declared twice, unregistered normalizer, or a `contracts.yml` `table` that disagrees
with the normalizer's `mainDestination` all fail before the first table is written. `contracts` and
the registry lookup (`FactoryLookup`) are both injectable parameters, so plan/validation is testable
without Spark. Execution itself is sequential with no cross-contract rollback — fine while every
contract destination is `OverWrite`. `clinvar` and `dbsnp` are wired; the other commands still
dispatch directly until they get contract entries.

## Test harness

`testutils/SparkSpec` = `AnyFlatSpec` + `Matchers` + `WithSparkTestEnvironment`, which builds a
`local` SparkSession over a **Hadoop** Iceberg catalog rooted at `./tmp/warehouse` and rebuilds
`conf` with LOCAL storages (raw = test resources dir). Because `SparkSession.getOrCreate` is a
singleton, the tmp dir is fixed and wiped in `beforeEach`/`afterEach` — do not introduce per-test
warehouse paths. Mix in `CreateDatabasesBeforeAll` (creates the `reference` database) and
`CleanUpBeforeAll` (drops tables + removes locations) when a spec loads to Iceberg.

Expected input/output models (`RawClinvar`, `NormalizedClinvar`, `EnrichedGenes`, …) live **upstream**
in `bio.ferlab:datalake-test-utils`, not in this repo. A new table generally needs its model added
there, or a locally-defined case class in the spec. Jobs under test are constructed with
`TestETLContext()`; call `transformSingle`/`loadSingle` directly rather than `run()`.

Tests are serial and forked (`parallelExecution := false`, `fork := true`) — Spark singleton plus a
shared tmp warehouse make parallel runs unsafe.

## Scala Stack & build constraints

- Scala 2.12, Spark 3.5, Iceberg, Glow, `bio.ferlab:datalake-spark3`, Java 11 (CI pins 11) — exact
  versions live in `spark/build.sbt`; read it rather than restating pins.
- `spark-sql`, `hadoop-client`, and `jackson-module-scala` are `Provided` — they ship in the Spark 3.5
  / EMR runtime (`jackson-module-scala` and `jackson-databind` are spark-core dependencies). Promoting
  them to compile scope bloats the JAR and causes Jackson version conflicts on EMR. `hadoop-aws`, the
  Iceberg bundles and `jackson-dataformat-yaml` are *not* Provided and must stay in the JAR —
  YAML is **not** a Spark dependency (it only reaches a distro's `jars/` via the fabric8 Kubernetes
  client), so `Provided` there means `contracts.yml` parsing dies on EMR with a `NoClassDefFoundError`
  that no local test can reproduce, since sbt puts `Provided` on the test classpath. `jacksonVersion`
  must still track Spark's Jackson (3.5.5 → 2.15.2) so the bundled YAML module matches the provided
  databind.
- `javaOptions` pin `user.language=en` / `user.country=US`: a French locale makes Glow float parsing
  (`3,00`) throw `NumberFormatException`.
- `shapeless` is shaded (`shadeshapless.@1`); `commons-logging` is globally excluded; several
  `META-INF` merge strategies are hand-tuned. Touch these only with a concrete assembly conflict.
- `.sbtopts` raises heap to 4G and stack to 5M — Spark codegen and wide VCF schemas need it.

---

# Shared

## Adding a New Data Source

1. Add a `SourceConfig` entry in `airflow/opendatalake/lib/domain/model/sources.py` inside `_Source`
2. Create a corresponding Spark normalization class in `spark/src/main/scala/org/radiant/opendatalake/normalized/`
3. Register the table in `EtlConfiguration.scala` and regenerate configs via `sbt runMain`
4. Register the Spark command in `ImportPublicTable.scala`
5. Declare the source's data contract in `spark/src/main/resources/contracts.yml` (SJRA-1546):
   `lineage` (`{MAJOR}.{MINOR}`), `table`, normalizer FQCN, `release_notes` path
6. Add a spec under `spark/src/test/scala/.../normalized/` (see Test harness above)

## Known rough edges

- `EtlConfiguration` only writes `prd.conf` and `test.conf`, but Airflow passes
  `config/<environment>.conf` (and `spark/sandbox/README.md` references `config/qa.conf`). Non-prd
  environments need their `StorageConf` list and a `ConfigurationWriter.writeTo` line added.
- `raw_gnomad_genomes_v3` is pinned to storage id `gnomad`, for which no `StorageConf` exists in
  `prd_storage` — that dataset cannot be read until the storage is declared.
- Glue-specific catalog properties are intentionally absent from the generated conf; they are injected
  at deploy/runtime by the Airflow operator's Spark conf.

## Documentation

`doc/` holds longer-form docs: `doc/poc/` (proof-of-concept write-ups per ticket, e.g. EMR Serverless, Iceberg branching), `doc/implementation-manuals/` (operational guides), and `doc/usage/` (operator usage guides). The architecture ADR is linked from the root `README.md`.
