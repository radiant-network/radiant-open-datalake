# Storage Convention

This document describes the naming conventions and S3 layout used for storing raw and processed data in the Radiant Open Datalake project.


## Iceberg Catalog and Database Naming

Both are **per-environment configuration**, not fixed by the code. The values below are what every
environment happens to use today.

- **Catalog name:** `opendatalake` — set at launch by `spark.sql.catalog.<name>.*` and
  `spark.sql.defaultCatalog`. The ETL never writes a catalog-qualified identifier, so the catalog is whatever
  `defaultCatalog` points at: Glue on EMR, Polaris in the sandbox, a Hadoop catalog in tests.
- **Database (namespace):** `reference` — `table.database` in `config/<ENV>.conf`, generated from
  `EtlConfiguration.iceberg_database`.
- **Table name:** for a source under a data contract, `<table_prefix>_v<MAJOR>` — so `clinvar_v1`, and a
  future `clinvar_v2` beside it. `table_prefix` is declared per source in `contracts.yml`.

Here is an example query syntax with Spark SQL:
```sql
SELECT * FROM opendatalake.reference.clinvar_v1;
```

or
```sql
USE CATALOG opendatalake;
SELECT * FROM reference.clinvar_v1;
```

### Reading a contract-managed table

Both queries above return **zero rows**. Contract-managed tables publish each `dataset_version` on its own
Iceberg branch and leave `main` empty, so a read has to name the branch:

```sql
SELECT * FROM opendatalake.reference.clinvar_v1.`branch_20260715`;
```

The backticks are required: a `dataset_version` is whatever the upstream source calls its release — all digits
for ClinVar (`20260715`), a RefSeq accession containing a dot for dbSNP (`GCF_000001405.40`) — so neither is a
valid bare SQL identifier. `VERSION AS OF '20260715'` does **not** work: Spark reads an all-digit version as a
snapshot id and fails with `Cannot find snapshot with ID 20260715`.


## S3 Layout

Here is the expected S3 layout:
```
s3://opendatalake-<ENV>/
 ├── raw/landing/<SOURCE>      # Raw data for each source
 └── iceberg/<DATABASE>/       # Iceberg tables, where <DATABASE> matches the database name (e.g.: reference)
```

- `<ENV>` is the environment tag, such as `qa`, `staging`, or `prod`. It also selects the configuration file the job is launched with (`--config config/<ENV>.conf`).
- Raw source data is stored under `raw/landing/<SOURCE>`.
- All Iceberg-managed tables are stored under `iceberg/<DATABASE>/`, matching the chosen database name — `EtlConfiguration` keeps the two in step by building the Iceberg storage root as `s3a://opendatalake-<ENV>/iceberg/${iceberg_database}`. This convention is inspired by Polaris, which requires this structure. It might not be strictly enforced by all catalog types (e.g., AWS Glue), but we believe it is a clear and future-proof convention.


## Future Considerations

We can adjust this structure in the future if it turns out to be incompatible with AWS Glue, operational requirements, or broader CHOP standards.

**Last updated:** August 2026