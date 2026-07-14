---
source: src/adapter/src/catalog/migrate.rs
revision: e6fbabee58
---

# adapter::catalog::migrate

Implements AST-level catalog migrations that rewrite stored SQL definitions when the SQL syntax changes incompatibly between versions.
`migrate` is called during catalog open and applies an ordered list of per-item rewrites (e.g. updating `CREATE VIEW` or `CREATE SINK` option names) by parsing, transforming, and re-serialising the stored SQL text.
`durable_migrate` includes `migrate_builtin_tables_to_mvs`, which updates persisted system-object mappings for builtins whose type changed from `Table` to `MaterializedView`.
`ast_rewrite_kafka_metadata_refresh_intervals` normalizes `TOPIC METADATA REFRESH INTERVAL` option values for Kafka sources and sinks to canonical Duration format.
`ast_rewrite_small_commit_intervals` rewrites any persisted `COMMIT INTERVAL` on Iceberg sinks that is below the 1 second planning minimum to `'1s'`, so that sinks created before the floor was enforced continue to plan successfully after an upgrade.
