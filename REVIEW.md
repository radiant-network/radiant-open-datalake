# Code Review

_Branch `feat/sjra-1656-implement-import-source` vs `main` — 19 files changed, +~250/-2547 (union of committed + working tree)._

_Verified by building: `sbt compile` **fails** (2 errors); `make test` (Python) passes 116/116._

## Critical
- [x] ✅ `spark/.../config/EtlConfiguration.scala:54` — FIXED: inlined `"{{VERSION}}"` in both raw paths (matching `RawInput`'s literal), dropping the dangling `RawInput.VersionPlaceholder` reference. `sbt clean test` green (17/17); generated configs unchanged.

## Medium
- [x] ✅ `spark/.../config/{RawInput.scala:22, EtlConfiguration.scala:54}` — MITIGATED: added a comment at the `EtlConfiguration` sites noting the token must stay identical to `RawInput.replacePath`. (Two bare literals remain by design — no shared constant.)
- [ ] 🟡 `spark/src/main/resources/config/` — SKIPPED (needs user confirmation): `qa.conf`/`staging.conf` deleted, only `prd.conf` generated. Confirm qa/staging EMR deploys are retired before merge.

## Low
- 🔵 `spark/.../ImportPublicTable.scala:17` — No CI test exercises the mainargs `--version`/`--raw-storage` dispatch; a wrong arg name surfaces only on EMR at runtime.
- 🔵 `airflow/opendatalake/lib/config.py:19` — Moving to `raw/landing/...` orphans data already written under the old `raw/<src>/<ver>/` layout; note a migration if any env is populated.
- 🔵 `spark/.../normalized/ClinvarSpec.scala:25` — `rawStorage = ""` is inert (extract bypassed); harmless but `version = "test"` vs `""` is an arbitrary inconsistency.

## Principle audit
**✅ Survives**
- `spark/.../config/RawInput.scala:15-18` — SOLID/DRY · overriding storage via the dataset's own `storageid` (instead of a hardcoded `RawStorageId`) removes a magic string and generalizes to any raw storage. Good change.
- `airflow/opendatalake/dags/import_source.py:46` — KISS · the `get_version` XComArg is passed straight into `entry_point_arguments`; Airflow wires the dependency and resolves the value — no task-id string, no `{{ }}` template. Verified `run_spark_import` depends on `get_version`.
- `airflow/opendatalake/lib/config.py` — DRY · `raw_landing_prefix`/`raw_storage_uri` are the single prefix discover/download/import and the Spark raw root share; env-overridable via `OPENDATALAKE_RAW_*`.

**❌ Dies**
- None.

**🔧 Needs adjustment**
- `spark/.../config/EtlConfiguration.scala` ↔ `RawInput.scala` — DRY/KISS · the `VersionPlaceholder` decision is still half-applied and uncompilable. Land one shape and stop toggling: keeping the literal inline in `RawInput` is fine — just inline it in `EtlConfiguration`'s two paths too, so no file references a `VersionPlaceholder` symbol.

## Summary
Not mergeable: the Scala side does not compile — `EtlConfiguration` points at `RawInput.VersionPlaceholder`, which was removed. One decision fixes it: inline `"{{VERSION}}"` in the two `EtlConfiguration` paths (matching the current `RawInput`), or restore the val in `RawInput`. Python side is clean and green.
