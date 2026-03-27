---
source: src/adapter/src/catalog/open/builtin_schema_migration.rs
revision: af9155582e
---

# adapter::catalog::open::builtin_schema_migration

Implements schema migration for builtin storage collections whose persist shard schemas change between versions.
Two migration mechanisms are provided: `Mechanism::Evolution` uses persist's schema evolution to evolve a shard in-place (backward-compatible changes only), and `Mechanism::Replacement` creates a new shard (works for all changes but discards data).
The `MIGRATIONS` list declares per-version migration steps; `Migration::run` applies outstanding steps, coordinating shard creation, schema registration, fingerprint updates, and cleanup across 0dt-compatible read-only and leader environments.
`Builtin::MaterializedView` is supported alongside `Table`, `Source`, and `ContinualTask` as a valid target for schema migrations.
