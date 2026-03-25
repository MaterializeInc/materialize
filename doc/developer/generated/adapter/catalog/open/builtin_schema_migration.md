---
source: src/adapter/src/catalog/open/builtin_schema_migration.rs
revision: b00f8bffd2
---

# adapter::catalog::open::builtin_schema_migration

Implements schema migration for builtin storage collections whose persist shard schemas change between versions.
Two migration mechanisms are provided: `Mechanism::Evolution` uses persist's schema evolution to evolve a shard in-place (backward-compatible changes only), and `Mechanism::Replacement` creates a new shard (works for all changes but discards data).
The `MIGRATIONS` list declares per-version migration steps; `Migration::run` applies outstanding steps, coordinating shard creation, schema registration, fingerprint updates, and cleanup across 0dt-compatible read-only and leader environments.
