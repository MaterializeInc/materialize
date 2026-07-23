---
source: src/sql/src/plan/statement/ddl.rs
revision: b6b85cfcef
---

# mz-sql::plan::statement::ddl

Plans all DDL statements; the root file covers the full breadth of catalog-modifying statements, while the `connection` submodule focuses on connection-type-specific option parsing and validation.
Iceberg sinks support `MODE UPSERT` and `MODE APPEND`; append mode prohibits a KEY and rejects source columns that conflict with system-managed append columns. Range types are permitted as Iceberg sink key columns.
The `iceberg_sink_builder` function accepts an optional `storage_connection: Option<ResolvedItemName>` for the AWS storage credentials; when present it must resolve to a `Connection::Aws` item; when absent the resulting `IcebergSinkConnection` carries `storage_connection_id: None`.
`REFRESH EVERY` intervals are validated to be at least 1 ms; intervals smaller than 1 ms produce a `PlanError`.
`TOPIC METADATA REFRESH INTERVAL` for Kafka sources and sinks is validated to be between 1 second and 1 hour (inclusive); intervals outside this range produce a planning error.
`SourceExportStatementDetails::Postgres` carries a `cast_oid_full_range: bool` field; `plan_create_subsource` passes it through to `generate_column_casts` to control whether OID-based casts cover the full range.
`plan_create_connection` dispatches on `CreateConnectionType::GlueSchemaRegistry` to plan `CREATE CONNECTION ... FOR AWS GLUE SCHEMA REGISTRY`, guarded by the `ENABLE_GLUE_SCHEMA_REGISTRY` feature flag.
`plan_alter_connection` maps `Connection::Gcp(_)` to `CreateConnectionType::Gcp`.
`AvroSchema::Glue { connection, seed, .. }` in a source format is fully planned: the connection is resolved (must be a `Connection::GlueSchemaRegistry` item) and the seed (populated by purification) is required. In a sink format (`kafka_sink_builder`), `AvroSchema::Glue` is handled separately: it resolves the connection, uses per-side `KEY`/`VALUE` schema name and compatibility-level options (rejecting the source-only `SCHEMA NAME`), generates the Avro schema via `AvroSchemaGenerator`, and produces `WireFormat::Glue { registry: Some(glue_connection_id) }`.
`plan_alter_cluster` rejects a `WAIT` clause when no shape dimension (`SIZE`, `AVAILABILITY ZONES`, or `INTROSPECTION`) is being changed: there is no hydrate-overlap to wait on.
`plan_auto_scaling_strategy` converts a `ClusterAutoScalingStrategyOptionValue` into an `Option<AutoScalingStrategy>`: an empty block (no sub-policies) maps to `None` (autoscaling disabled). `validate_auto_scaling_strategy` checks cross-config invariants: it rejects a `HYDRATION SIZE` equal to the cluster `SIZE` (producing `PlanError::HydrationSizeEqualsClusterSize`), and rejects combining `AUTO SCALING STRATEGY` with a non-`MANUAL` `SCHEDULE`. `unplan_auto_scaling_strategy` is the reverse, converting an `AutoScalingStrategy` back to a `ClusterAutoScalingStrategyOptionValue` for `SHOW CREATE CLUSTER`. `AUTO SCALING STRATEGY` is gated by the `ENABLE_AUTO_SCALING_STRATEGY` feature flag for new DDL, but `RESET (AUTO SCALING STRATEGY)` is intentionally not gated so a cluster can shed an autoscaling policy after a flag rollback. `AUTO SCALING STRATEGY` is rejected for unmanaged clusters.
`plan_alter_sink` handles `AlterSinkAction::SetOptions` and `AlterSinkAction::ResetOptions`, currently restricted to the `CommitInterval` option name. A `SET` identical to the current with-options returns `Plan::AlterNoop`. A `RESET` of an option that is not set is rejected.
`iceberg_sink_builder` enforces a minimum `COMMIT INTERVAL` of 1 second; intervals shorter than 1 second produce the error `"COMMIT INTERVAL must be at least 1 second"`.
`plan_create_type` validates nested type references using a shared `TypeResolutionBudget`, rejecting types that exceed the nesting depth limit (128) or total resolution node limit (100,000) with graceful planning errors.
