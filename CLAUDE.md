# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**radiant-open-datalake** is a monorepo with two independent components:
- **`airflow/`** — Python orchestration for discovering and ingesting public genomic datasets
- **`spark/`** — Scala transformations that normalize/enrich raw data into Iceberg tables on S3

## Airflow Commands

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

## Spark Commands

All commands run from the `spark/` directory.

```bash
sbt clean test     # Run all Scala tests
sbt assembly       # Build fat JAR for deployment
sbt "runMain org.radiant.opendatalake.config.EtlConfiguration"  # Regenerate environment configs
```

## Architecture

### Data Flow

```
External Sources (ClinVar, gnomAD, etc.)
    → Airflow DAG: discover-new-source-versions (daily 6AM UTC)
        → S3 raw landing: s3://opendatalake-<ENV>/raw/landing/<SOURCE>/<VERSION>/
    → Spark ETL jobs (triggered by DAG)
        → Iceberg tables: s3://opendatalake-<ENV>/iceberg/reference/
            → Iceberg catalog: opendatalake.reference.<table>
```

### Airflow DAG Structure

The main DAG (`discover_new_source_versions.py`) uses Airflow 3.0.6 asset-based dependencies. Sources are defined declaratively:
- **`SourceConfig`** — describes a dataset source (URL, versioning strategy, update mode)
- **`DownloadConfig`** — describes download parameters
- **`_Source` enum** (`sources.py`) — registry of all configured sources; add new sources here

S3 transfers use multipart upload with resume support (`lib/s3_transfer.py`). The Airflow connection ID for S3 is `aws_default`; bucket name comes from the `environment` Airflow variable.

### Spark ETL Structure

- **`ImportPublicTable.scala`** — command registry; each dataset is a named `App` object
- **`EtlConfiguration.scala`** — all dataset table definitions and S3 paths; generates env-specific configs
- **`normalized/`** — one class per dataset, reads raw files, produces standardized Iceberg table
- **`enriched/`** — joins/aggregates normalized tables

The ETL framework is `bio.ferlab:datalake-spark3` (FerLab commons). All jobs use `RuntimeETLContext` for configuration. Genomics processing uses Glow 2.0.0.

### Python Stack

- Python 3.12 (version-pinned; critical for AWS deployment compatibility)
- Airflow 3.0.6 with `airflow-providers-amazon` 9.12.0
- Dependencies locked via `airflow/constraints-python3.12.txt`
- Linter: Ruff (line length 119, rules: E, F, UP, B, SIM, I) — config at `airflow/.ruff.toml`
- Tests: pytest, config at `airflow/pytest.ini` (pythonpath includes `dags/`)

### Scala Stack

- Scala 2.12.18, Spark 3.5.5, Iceberg 1.10.1, Java 11+
- Tests run non-parallel (`parallelExecution := false` in `build.sbt`)

## Adding a New Data Source

1. Add a `SourceConfig` entry in `airflow/dags/lib/domain/model/sources.py` inside `_Source`
2. Create a corresponding Spark normalization class in `spark/src/main/scala/org/radiant/opendatalake/normalized/`
3. Register the table in `EtlConfiguration.scala` and regenerate configs via `sbt runMain`
4. Register the Spark command in `ImportPublicTable.scala`
