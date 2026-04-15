---
source: src/adapter/src/catalog/open/builtin_schema_migration.rs
revision: 8dfe3daa00
---

# adapter::catalog::open::builtin_schema_migration

Implements schema migration for builtin storage collections whose persist shard schemas change between versions.
Two migration mechanisms are provided: `Mechanism::Evolution` uses persist's schema evolution to evolve a shard in-place (backward-compatible changes only), and `Mechanism::Replacement` creates a new shard (works for all changes but discards data).
The `MIGRATIONS` list declares per-version migration steps; `Migration::run` applies outstanding steps, coordinating shard creation, schema registration, fingerprint updates, and cleanup across 0dt-compatible read-only and leader environments.
`CatalogItemType::MaterializedView` is supported alongside `Table`, `Source`, and `ContinualTask` as a valid target for schema migrations, as reflected by entries in the `MIGRATIONS` list that migrate several `mz_catalog` and `mz_internal` materialized views including `mz_databases`, `mz_schemas`, `mz_role_members`, `mz_network_policies`, `mz_network_policy_rules`, `mz_cluster_workload_classes`, `mz_internal_cluster_replicas`, `mz_pending_cluster_replicas`, and `mz_materialized_views`.
