---
source: src/adapter/src/catalog/migrate.rs
revision: d08120c52d
---

# adapter::catalog::migrate

Implements AST-level catalog migrations that rewrite stored SQL definitions when the SQL syntax changes incompatibly between versions.
`migrate` is called during catalog open and applies an ordered list of per-item rewrites (e.g. updating `CREATE VIEW` or `CREATE SINK` option names) by parsing, transforming, and re-serialising the stored SQL text.
`durable_migrate` includes `migrate_builtin_tables_to_mvs`, which updates persisted system-object mappings for builtins whose type changed from `Table` to `MaterializedView`.
`ast_rewrite_kafka_metadata_refresh_intervals` normalizes `TOPIC METADATA REFRESH INTERVAL` option values for Kafka sources and sinks to canonical Duration format.
`ast_rewrite_small_commit_intervals` rewrites any persisted `COMMIT INTERVAL` on Iceberg sinks that is below the 1 second planning minimum to `'1s'`, so that sinks created before the floor was enforced continue to plan successfully after an upgrade.
`ast_rewrite_strip_builtin_version_pins` strips the `VERSION` qualifier from by-id references to non-user (builtin) items. A version pin on a builtin is meaningless since builtins are not user-versioned; if the builtin is later converted to an item type without versions (e.g. from a table to a materialized view), reparsing the persisted pin fails with `InvalidVersion` and panics during catalog open. Stripping it makes the reference resolve to the latest version. Safe to run on every boot; stripping an absent version is a no-op.
