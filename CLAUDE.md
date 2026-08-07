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

**Airflow pools are environment state, not code.** `config.py` names two — `DOWNLOAD_TASKS_POOL` for
the ECS path, `DIRECT_UPLOAD_TASKS_POOL` for `direct_upload` — but they must be created in the target
Airflow (Admin → Pools) or the tasks claiming them never run. They are deliberately separate: an ECS
task only polls, whereas `direct_upload` streams the file *inside the Airflow worker* and holds a
`part_size_mb` chunk (200 MB) in memory per running task, so its pool is sized against worker memory
across all sources. Documented in `airflow/README.md` under Runtime Requirements.

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
sbt "Test/runMain org.radiant.opendatalake.config.EtlConfiguration"  # regenerate resources/config/*.conf
```

`Test/` is not a typo: `EtlConfiguration` reads `contracts.yml` to name the contract-managed tables, and
`jackson-module-scala` is `Provided`, so the parser is on the test classpath but not the compile one. Plain
`Compile/runMain` dies with `NoClassDefFoundError: DefaultScalaModule`.

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
(`s3a://opendatalake-<env>/iceberg/<database>`).

Catalog and database are **per-environment configuration**, both `opendatalake` / `reference` today but
neither fixed by the code. The database is `TableConf.database` in `config/<env>.conf`, from
`EtlConfiguration.iceberg_database` — which also builds the `iceberg_storage` root, so renaming it moves
where tables land. The catalog name appears nowhere in the ETL: every identifier it emits is
`<database>.<table>`, and `spark.sql.defaultCatalog` at launch decides which catalog that is. That is what
lets one JAR run against Glue on EMR (`operators/emr.py`), Polaris in the sandbox
(`operators/spark_k8s.py`) and a Hadoop catalog in tests (`WithSparkTestEnvironment`). The consumer-facing
version of this, including how to read a branch, is `spark/doc/storage_convention.md`.

`test.conf` deliberately rewrites every Iceberg dataset path to `/<table-name>`: the local Hadoop
catalog used in tests rejects custom table locations — `HadoopCatalogTableBuilder.withLocation` compares
against `<warehouse>/<namespace>/<table>` with `String.equals` and throws on anything else. Keep that
mapping intact when touching `EtlConfiguration`. Since `WapLoader` creates tables *at* the declared
location, `IcebergTable.createEmpty` qualifies it first (`/x` → `file:/x`, `s3a://…` untouched) — a LOCAL
`StorageConf` root is a bare path, and unqualified it would differ from that catalog default by exactly
`file:`.

## Job shape

Every job is a `case class Xxx(rc: RuntimeETLContext)` extending a FerLab base
(`bio.ferlab.datalake.spark3.etl.v4`) and paired with a companion `object` exposing a `@main run` —
except the contract-managed ones (`Clinvar_v1`, `DBSNP_v1`, `GnomadCNV_v1`, `GnomadJoint_v1`,
`GnomadSV_v1`, `Mondo_v1`, `OneThousandGenomes_v1`), which have no companion
at all: a second `@main` would be a launch path that skips contract selection and the destination
check, so their only entry point is `ImportPublicTable` dispatching through `ContractRunner`.

- `SimpleETLP` — normalized jobs (13 of them); publishes/partitions per the `DatasetConf`.
- `SimpleSingleETL` — enriched jobs and `DBNSFPRaw` (4).
- `contracts.ContractETLP` — the seven contract-managed jobs (`Clinvar_v1`, `DBSNP_v1`, `GnomadCNV_v1`,
  `GnomadJoint_v1`, `GnomadSV_v1`, `Mondo_v1`, `OneThousandGenomes_v1`). It derives their
  destination per MAJOR (see **Data contracts** below) and extends `wap.WapETLP`, a `SimpleETLP` whose
  `loadSingle` publishes by Iceberg branch instead of overwriting the table; see **Write-Audit-Publish**
  below. Nothing extends `WapETLP` directly except a test fixture.

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

## Write-Audit-Publish by Iceberg branch (`wap/`, SJRA-1546 §2.1)

Two layers, both under `wap/`. `wap/iceberg/` owns **all** the Iceberg SQL and mechanics as one value type,
`IcebergTable(database, name)`: ref lookups, branch DDL, branch-scoped reads/writes, bootstrap. It keeps every
awkward detail private — ref-name quoting, the fact that only a branch-qualified identifier honours a branch
on write, and the mandatory `REFRESH` before a ref read — so a caller works in branches and snapshots and
never assembles an identifier. `IcebergTable.fullName` is deliberately *not* FerLab's `TableConf.fullName`,
which drops the database when it is empty and would yield an unqualified identifier.

**The database is not created here.** It is infrastructure, provisioned before the ETL runs, so nothing under
`wap/` issues `CREATE DATABASE` — a missing `reference` namespace is meant to fail the import loudly rather
than be conjured mid-run. Specs that load to Iceberg mix in `CreateDatabasesBeforeAll` to stand it up.

`wap/WapLoader.scala` holds the flow and contains no SQL of its own; it is nothing but the steps of §2.1:
`prepareCleanBase` → `stageOnAuditBranch` → `publishVersionBranch`. New Iceberg verbs (e.g. §2.2 tagging) go
on the `wap.iceberg` types; new flow logic goes in `wap`.

Every contract-managed job reaches this through `contracts.ContractETLP`, which extends `wap.WapETLP`; the
latter overrides `loadSingle` to publish through `wap.WapLoader` instead of the framework's load path. Per run, on the destination table: reset a
transient `audit_{version}` branch to `main`, write there, `CREATE OR REPLACE BRANCH {version}` at the
resulting snapshot, drop the audit branch. **`main` is left permanently empty** — it is only a clean base
to branch from, and the published data lives on a branch named *exactly* the `dataset_version`
(clinvar `20260715`, dbsnp `GCF_000001405.40`). Re-importing a version replaces its branch (§3.4).

Consequences worth knowing before touching any of this:

- **Consumers must name a branch.** `SELECT … FROM opendatalake.reference.clinvar_v1` with no branch reads
  `main` and returns zero rows. Nothing in this repo reads these two tables, but StarRocks/portal do. In SQL
  only the branch-qualified identifier works — ``reference.clinvar_v1.`branch_20260715` ``. `VERSION AS OF
  '20260715'` does *not*: Spark reads an all-digit ref as a snapshot id and fails with `Cannot find snapshot
  with ID 20260715`.
- **The FerLab load path cannot be reused.** `ETL.loadDataset` ends at DataFrameWriter V1
  (`mode(Overwrite).saveAsTable`), which Spark resolves to `ReplaceTableAsSelect` — a staged table
  *replace*, which discards refs. Same reason the `spark.wap.branch` session conf is not an option here:
  Iceberg honours it for append/overwrite-by-expression, not for a staged replace. Bypassing
  `loadDataset` means `WapETLP` must redo its `repartition.getOrElse(defaultRepartition)`; its
  `CREATE DATABASE IF NOT EXISTS` is deliberately *not* carried over.
- **Writes must use the branch-qualified identifier**, `df.writeTo("db.t.`branch_x`")`.
  `writeTo(t).option("branch", x)` is silently ignored on the write path and commits to `main` instead;
  the `branch` option *does* work for reads (`spark.read.option("branch", x).table(t)`).
- Ref names are backtick-quoted everywhere: clinvar's versions are all-digits and dbsnp's contains a dot,
  so neither is a valid bare SQL identifier. Iceberg itself does not validate ref names.
- `REFRESH TABLE` before reading `.refs` — `SparkCatalog` caches metadata (30s default TTL) and every
  step commits then immediately reads the ref it just moved.
- A zero-row `writeTo(...).create()` still commits an empty append snapshot, which is what the first
  `CREATE BRANCH` points at. That is how a source bootstraps its table with an empty `main`.
- **`main` is emptied if it is not already**, and the audit write is an `overwrite(lit(true))` rather than
  an `append`. Both matter: audit is cut *from* `main`, so on a table written by the old overwrite path
  (data on `main`) an append inherited those rows and published **2x** on the version branch while `main`
  kept the originals. `WapLoader` now issues `DELETE FROM <table>` when `main` has rows — schema-agnostic,
  so a legacy table whose schema has since evolved still empties — and the overwrite makes the audit branch
  equal to exactly the incoming data regardless of what it inherited. Already-published version branches
  are untouched; they are independent refs.
- **The table is created at `DatasetConf.location`**, passed as the `location` create property
  (`TableCatalog.PROP_LOCATION`, which Iceberg's `SparkCatalog` reads). Omitting it does not fail — the
  catalog quietly picks its own default (Glue: `<warehouse>/<db>.db/<table>`), which is *not* where
  `EtlConfiguration` says the dataset lives. The FerLab load path this replaced passed it as
  `.option("path", location).saveAsTable(...)`, so dropping it would have silently relocated any newly
  bootstrapped table.
- **Schema evolution on write needs two settings, and neither works alone** — this is what lets a MINOR
  contract bump add columns instead of failing the import. `merge-schema` (write option, set on the audit
  write) is what evolves the table: Iceberg's `SparkWriteBuilder.build()` calls `validateOrMergeWriteSchema`
  → `updateSchema().unionByNameWith(incoming).commit()`. But Spark's analyzer runs first and
  `TableOutputResolver` rejects the extra column with `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS`
  before the connector is ever asked to build a write. `write.spark.accept-any-schema` (table property, set
  at `createEmpty` and back-filled by `prepareCleanBase` on older tables) is what gets past that: Iceberg's
  `SparkTable` advertises `ACCEPT_ANY_SCHEMA` only when it is set, which flips
  `DataSourceV2Relation.skipSchemaResolution`. Drop the option and the write dies with
  `Field <col> not found in source schema`; drop the property and it dies in analysis. Matching is by name,
  so column order is irrelevant and a dropped column reads back null. Iceberg's schema is table-level, not
  per-branch, so widening shows the new column on already-published branches — with no value behind it.
- `ETL.run()` returns `conf.getDataset(id).read`, i.e. `main`, so its return value is empty for these
  jobs. Nothing consumes it.
- Extending `WapETLP` *is* the guarantee that a job cannot publish to `main` — directly, or via
  `ContractETLP` as the contract jobs do: `SingleETL` makes `transform`/`load` final, so `loadSingle` is the
  only seam and it is already overridden.

## Data contracts (`config/contracts/` + `contracts/`, SJRA-1546 / SJRA-1747)

Split by role, and keep it that way: **`config.contracts` is what `contracts.yml` declares** — the yaml model
and loader, i.e. `Contracts.scala` and nothing else — while **`contracts` is what executes it** —
`ContractDestination`, `ContractETLP`, `ContractRegistry`, `ContractRunner`, `NormalizerArgs`, which build
runtime `DatasetConf`s or touch `ETL` types, Spark and `RuntimeETLContext`, and so have no business under
`config`.
Dependencies run one way, `contracts` → `config.contracts`, and `wap` still knows nothing about either. Two
packages end up named `contracts`, so always import fully qualified — a relative `contracts.X` resolves
against the enclosing package in Scala 2.

`spark/src/main/resources/contracts.yml` is the declared source of truth for which contracts the ETL
must execute: per source a `table_prefix`, and per MAJOR a row carrying a `lineage` `"{MAJOR}.{MINOR}"` and a
`release_notes` path (relative to `spark/`, under `spark/doc/release-notes/<source>/v<MAJOR>.md`, Keep a
Changelog format). MINOR is bumped in place when columns are added by schema evolution; a new MAJOR is a new
row. The yaml deliberately carries **no normalizer class name** — `(source, MAJOR)` is the registry key, so
the implementing class is named once, in Scala, where the compiler checks it.

**`table_prefix` names the table and `lineage` supplies the MAJOR; nothing else names anything.**
`contracts/ContractDestination` does the computing — `tableName(prefix, major)` gives `clinvar_v1`, and
`forMajor(family, prefix, major)` narrows the source's *one* `DatasetConf` into a per-MAJOR destination. The
prefix is what the name comes from, so a developer picks an arbitrary table name in the yaml and it need not
match the family's own; the family supplies the database, the storage, the partitioning, and the directory its
tables sit in. `EtlConfiguration` declares that family through **`buildNormalizedDatasetConf("clinvar", …)`**, which reads
`table_prefix` off `contracts.yml` rather than repeating it — so the name is written once, in the yaml, and
`clinvar_v1`, `clinvar_v2`, … are what actually get created, ingesting in parallel (§3.4). MAJOR 1 is not a
special case. Adding a MAJOR touches no configuration and needs no regenerate.

Because `EtlConfiguration` now parses the yaml, it must be run as **`sbt "Test/runMain …"`** — see the Commands
section above.

Consequences worth knowing:

- **The dataset id and the table name are deliberately different things, and are meant to diverge.** The id is
  the internal identifier of the data plus its MAJOR, so it is keyed on the *source*
  (`normalized_clinvar` → `normalized_clinvar_v1`); `table_prefix` is cosmetic, the published name, so a
  decorated prefix yields `clinvar_open_v1` on the same dataset. Nothing needs them to agree: the id is only
  ever the self-consistent map key in `toMain` / `load(data(mainDestination.id))`, nothing resolves a dataset
  by the derived id, and `ContractRunner.run` logs the table rather than the id.

- The derived name **replaces the family's last path segment**, whatever it happens to be, rather than being
  appended to the path — which is what lets one rule serve both environments: prd `/normalized/clinvar` →
  `/normalized/clinvar_v1`, and the test config (where `EtlConfiguration` rewrites every Iceberg path to `/` +
  table name) `/clinvar` → `/clinvar_v1`. Replacing rather than appending is why nothing needs validating: the
  location ends with the derived table name by construction, so the old `require` that the family path end with
  its table name is gone. Appending would instead have put `clinvar_v1` at `…/clinvar/clinvar_v1`.
- **A contract family's path must be exactly one segment deep in the test config**, because the local Hadoop
  catalog accepts only `<warehouse>/<namespace>/<table>`. That holds today for free: `test_conf` flattens every
  Iceberg path to `/` + table name. Nothing enforces it beyond that, so a family pathed like `normalized_dbnsfp`
  (`/normalized/dbnsfp/variant`) would derive `/normalized/dbnsfp/dbnsfp_v1` — correctly *named*, two levels
  deep, accepted by Glue and rejected by the Hadoop catalog. Unreachable while every family comes from
  `buildNormalizedDatasetConf`, which emits `/normalized/<prefix>`.
- A contract normalizer extends **`ContractETLP(rc, sourceDatasetId, tablePrefix, major)`** and no longer
  overrides `mainDestination` — it states which family it reads and which MAJOR it implements, while the prefix
  arrives as *data*, resolved from the yaml by `ContractRunner` and carried in `NormalizerArgs`, so a class
  cannot name its own table. Derived lazily: `conf` belongs to the `ETL` constructor. Overriding
  `mainDestination` anyway is the escape hatch for a MAJOR needing partitioning of its own; the name is still
  checked. `build` therefore takes `(rc, version, rawStorage)` rather than a ready-made `NormalizerArgs` —
  the prefix is resolved inside it, so a caller is not in a position to supply one.
- The MAJOR is a literal in the normalizer rather than injected from the registry key **on purpose**: a class
  that accepted any MAJOR would let a mistyped registry entry publish its schema under another contract, where
  a fixed one makes that a `destinationMismatchReason` failure before any write.
- Two guardrails: `destinationMismatchReason` (the job's destination is the table its `(table_prefix, MAJOR)`
  implies — catches a wrong family id or a wrong MAJOR literal), and `ContractFanOutSpec`'s **`build` over the
  real `contracts.yml` and registry**. That second one exists because `plan` resolves factories without
  invoking them and so never sees a destination: a MAJOR 2 class left declaring `major = 1` publishes into
  MAJOR 1's table, `plan` passes, and only `build` catches it — a gap that once let exactly that reach EMR.
  Building is enough; nothing is extracted or written. Checking against `test.conf` covers `prd.conf` too,
  since both are generated from the same `sources` list, stale regeneration included.
- `plan` rejects a `table_prefix` that already carries a `_v<digits>` suffix — it would derive `clinvar_v1_v1`.

`Contracts.load()` parses the file off the classpath with Jackson YAML (snake_case → camelCase,
`FAIL_ON_UNKNOWN_PROPERTIES` on, so a typo'd key throws — and so does a leftover `table:` on a row, which is
the migration guard). Jackson passes *null*, never an empty collection, for empty yaml keys, so `Contracts`
normalizes both a null `sources` map and a null per-source value — otherwise a half-written file NPEs instead
of reporting itself.

`release_notes` is the only untyped path left in the file; `ContractsSpec` asserts each one exists on
disk, and `ContractRegistrySpec` keeps the declared `(source, MAJOR)` pairs and the registry keys in
bijection.

**Fan-out.** Contract-declared sources dispatch through `ContractRunner.run(source, …)` instead of a
hard-coded job: the CLI command names the *source*, `contracts.yml` decides which normalizers run for
it (SJRA-1546 §3.2). Adding a MAJOR never touches the CLI or Airflow — see **Bumping a contract MAJOR**
below. `ContractRegistry` maps `(source, MAJOR)` to a factory — a registry rather than reflection because
normalizer constructor arities differ, and keyed on that pair because it is what identifies a contract: a
MAJOR the registry does not know must resolve to nothing rather than to the previous MAJOR's normalizer.
`plan` returns a `ContractPlan` carrying the validated `tablePrefix` alongside the resolved jobs, since the
table a contract publishes to is derived and nothing else can name it.
The fan-out builds and validates every job before running any (`ContractRunner.build`): unknown
source, a missing or already-suffixed `table_prefix`, a MAJOR declared twice, an unregistered normalizer,
or a job whose destination is not the table its MAJOR implies all fail before the first table is written.
`contracts` and the registry lookup (`FactoryLookup`) are both injectable parameters, so plan/validation is
testable without Spark. Execution itself is sequential with no cross-contract rollback — fine while every
contract destination is `OverWrite`. Each contract also re-extracts the raw input independently; §3.1
encourages sharing intermediate transforms between MAJORs of one source, which nothing does yet because
no source has two. `1000_genomes`, `clinvar`, `dbsnp`, `gnomad_cnv`, `gnomad_joint`, `gnomad_sv` and
`mondo` are wired; the other commands still dispatch directly until they get contract entries.

`ContractFanOutSpec` is the end-to-end proof: it declares two MAJORs of a fictional source through
`ContractRunner.run`'s injectable `contracts` / `factories` seams — no row in `contracts.yml`, no dataset
in `EtlConfiguration` — and asserts the two tables are independent, `main` stays empty on both, and one
unregistered row aborts the whole plan before anything is written.

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
3. Register the table in `EtlConfiguration.scala` and regenerate configs via `sbt "Test/runMain …"` (see Commands)
4. Register the Spark command in `ImportPublicTable.scala`
5. Declare the source's data contract in `spark/src/main/resources/contracts.yml` (SJRA-1546): a
   `table_prefix` for the source (the table family — step 3's table name, *without* a MAJOR suffix), then a
   row per MAJOR with `lineage` (`{MAJOR}.{MINOR}`) and a `release_notes` path. Map `(source, MAJOR)` to the
   normalizer in `ContractRegistry`, extend `ContractETLP` in the normalizer, and write the release notes
   file the row points at
6. Add a spec under `spark/src/test/scala/.../normalized/` (see Test harness above)

## Bumping a contract MAJOR

A MAJOR is a breaking schema change, so it gets a **new table** and the old MAJOR keeps ingesting beside it
(SJRA-1546 §3.4). No CLI, Airflow or operator change — the command names the source, not the contract.

**No `EtlConfiguration` change and no regenerate** — the source's single dataset family serves every MAJOR,
and `reference.<source>_v{N+1}` is derived from it. Three edits:

1. New row under the source's `contracts` in `contracts.yml`: `lineage: "{N+1}.0"` and `release_notes:
   "doc/release-notes/<source>/v{N+1}.md"`. Leave the old row and the `table_prefix` alone.
2. New normalizer class `<Source>_v{N+1}` beside the old one, `extends ContractETLP(rc, sourceDatasetId =
   "normalized_<source>", major = N+1)` — the old class is frozen, that is the point of the bump.
3. `ContractRegistry` entry mapping `("<source>", N+1)` to the new class.
4. Write `spark/doc/release-notes/<source>/v{N+1}.md` (Keep a Changelog; copy the v1 header table).
5. `sbt test` — `ContractRegistrySpec` and `ContractsSpec` fail loudly on any of these being missing or
   inconsistent. The table itself is created on the first run by `WapLoader`; nothing pre-creates it.

If the new MAJOR needs different partitioning, declare an explicit `DatasetConf` for `<source>_v{N+1}` and
override `mainDestination` to point at it — `destinationMismatchReason` still checks the name.

MINOR is different: bump `lineage` in place on the existing row and add columns to the *same* normalizer.
Iceberg schema evolution widens the existing table (see the two required settings under **Write-Audit-Publish**).

## Known rough edges

- `EtlConfiguration` only writes `prd.conf` and `test.conf`, but Airflow passes
  `config/<environment>.conf` (and `spark/sandbox/README.md` references `config/qa.conf`). Non-prd
  environments need their `StorageConf` list and a `ConfigurationWriter.writeTo` line added.
- Glue-specific catalog properties are intentionally absent from the generated conf; they are injected
  at deploy/runtime by the Airflow operator's Spark conf.
- **`split_multiallelics` in a `DatasetConf`'s `readoptions` does nothing.** Glow dropped the
  `splitToBiallelic` reader option in 0.4.0; splitting is a *transformer*
  (`Glow.transform("split_multiallelics", df)`, wrapped as `withSplitMultiAllelic`), and the `.read`
  path — `LoadResolver` → `GenericLoader.read` → `spark.read.options(...).format("vcf")` — has nowhere
  to apply one, so Spark silently ignores the unknown option. Only `GenomicImplicits.vcf(…, split =
  true)` actually splits, and its two-argument overload passes `split = false`. Harmless today: gnomAD
  and ClinVar release biallelic VCFs (gnomAD since v2.1, by policy), and `DBSNP_v1` explodes
  `alternateAlleles` itself. The option is still declared on several datasets here and in the lib.

## Documentation

`doc/` holds longer-form docs: `doc/poc/` (proof-of-concept write-ups per ticket, e.g. EMR Serverless, Iceberg branching), `doc/implementation-manuals/` (operational guides), and `doc/usage/` (operator usage guides). The architecture ADR is linked from the root `README.md`.
