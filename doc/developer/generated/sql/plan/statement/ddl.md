---
source: src/sql/src/plan/statement/ddl.rs
revision: 721951ce66
---

# mz-sql::plan::statement::ddl

The largest statement-planning file: implements `describe_*` and `plan_*` for all DDL statements — `CREATE`/`ALTER`/`DROP` for sources, sinks, views, materialized views, tables, indexes, connections, secrets, clusters, roles, schemas, databases, types, network policies, and more.
Delegates connection-specific logic to the `connection` submodule.
Exports `PgConfigOptionExtracted` and `PlannedRoleAttributes` for use by other modules.
Iceberg sink planning validates that key columns are primitive, non-floating-point types using an allow-list; columns outside the list produce a `PlanError::IcebergSinkUnsupportedKeyType`. Iceberg type overrides are applied via `iceberg_type_overrides` from `mz-storage-types`.
Iceberg sinks support two modes: `MODE UPSERT` (requires a key) and `MODE APPEND` (key is not allowed). `MODE APPEND` sinks reject input columns whose names conflict with the system columns `ICEBERG_APPEND_DIFF_COLUMN` and `ICEBERG_APPEND_TIMESTAMP_COLUMN` that the append mode writes to the Iceberg table.
