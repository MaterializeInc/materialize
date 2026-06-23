---
source: src/sql/src/plan/statement/ddl.rs
revision: b72bd8ad32
---

# mz-sql::plan::statement::ddl

The largest statement-planning file: implements `describe_*` and `plan_*` for all DDL statements — `CREATE`/`ALTER`/`DROP` for sources, sinks, views, materialized views, tables, indexes, connections, secrets, clusters, roles, schemas, databases, types, network policies, and more.
Delegates connection-specific logic to the `connection` submodule.
Exports `PgConfigOptionExtracted` and `PlannedRoleAttributes` for use by other modules.
`plan_create_connection` dispatches on `CreateConnectionType::GlueSchemaRegistry` to plan `CREATE CONNECTION ... FOR AWS GLUE SCHEMA REGISTRY`, guarded by the `ENABLE_GLUE_SCHEMA_REGISTRY` feature flag.
`plan_alter_connection` maps `Connection::Gcp(_)` to `CreateConnectionType::Gcp`.
Iceberg sink planning validates that key columns are primitive, non-floating-point types using an allow-list; columns outside the list produce a `PlanError::IcebergSinkUnsupportedKeyType`. Iceberg type overrides are applied via `iceberg_type_overrides` from `mz-storage-types`. Range types are permitted as Iceberg sink key columns.
Iceberg sinks support two modes: `MODE UPSERT` (requires a key) and `MODE APPEND` (key is not allowed). `MODE APPEND` sinks reject input columns whose names conflict with the system columns `ICEBERG_APPEND_DIFF_COLUMN` and `ICEBERG_APPEND_TIMESTAMP_COLUMN` that the append mode writes to the Iceberg table.
The `iceberg_sink_builder` function accepts an optional `storage_connection: Option<ResolvedItemName>` for the AWS storage credentials; when present it must resolve to an `Connection::Aws` item; when absent the resulting `IcebergSinkConnection` carries `storage_connection_id: None`.
`REFRESH EVERY` intervals are validated to be at least 1 ms; intervals smaller than 1 ms produce a `PlanError`.
`TOPIC METADATA REFRESH INTERVAL` for Kafka sources and sinks is validated to be between 1 second and 1 hour (inclusive); intervals outside this range produce a planning error (enforcing librdkafka runtime constraints at plan time for the upper bound, and preventing excessive refreshes or zero/negative durations for the lower bound).
`plan_role_variable` (used by `plan_alter_role`) accepts a `StatementContext` and calls `vars::check_transaction_isolation_feature_flag` on any `SET` assignment, enforcing the same feature-flag gate as the `SET` and connection-option paths.
`DROP … name1, name2, …` resolves all named items before running the non-cascade dependency check, so that two co-dependent items listed in the same statement do not block each other: dependents whose ids appear in the same drop set are excluded from `ensure_no_blocking_dependents`.
`AvroSchema::Glue { .. }` in a source format raises a "not yet implemented" error at planning time.
