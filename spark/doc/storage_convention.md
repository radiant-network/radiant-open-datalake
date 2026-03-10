# Storage Convention

This document describes the naming conventions and S3 layout used for storing raw and processed data in the Radiant Open Datalake project.


## Iceberg Catalog and Database Naming

- **Catalog name:** `opendatalake`
- **Database (namespace):** `reference`


Here is an example query syntax with Spark SQL:
```sql
SELECT * FROM opendatalake.reference.clinvar;
```

or
```sql
USE CATALOG opendatalake;
SELECT * FROM reference.clinvar;
```


## S3 Layout

Here is the expected S3 layout:
```
s3://opendatalake-<ENV>/
 ├── raw/landing/<SOURCE>      # Raw data for each source
 └── iceberg/reference/        # Iceberg tables, where "reference" matches the database name
```

- `<ENV>` is the environment tag, such as `qa`, `staging`, or `prod`.
- Raw source data is stored under `raw/landing/<SOURCE>`.
- All Iceberg-managed tables are stored under `iceberg/reference/`. Here "reference" matches the chosen database name. This convention is inspired by Polaris, which requires this structure. It might not be strictly enforced by all catalog types (e.g., AWS Glue), but we believe it is a clear and future-proof convention.


## Future Considerations

We can adjust this structure in the future if it turns out to be incompatible with AWS Glue, operational requirements, or broader CHOP standards.

**Last updated:** March 2026