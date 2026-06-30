---
source: src/adapter/src/catalog/open/builtin_schema_migration.rs
revision: e309d81832
---

# adapter::catalog::open::builtin_schema_migration

Implements schema migration for builtin storage collections whose persist shard schemas change between versions.
Two migration mechanisms are provided: `Mechanism::Evolution` uses persist's schema evolution to evolve a shard in-place (backward-compatible changes only), and `Mechanism::Replacement` creates a new shard (works for all changes but discards data).
The `MIGRATIONS` list declares per-version migration steps; `Migration::run` applies outstanding steps, coordinating shard creation, schema registration, fingerprint updates, and cleanup across 0dt-compatible read-only and leader environments.
`CatalogItemType::MaterializedView` is supported alongside `Table` and `Source` as a valid target for schema migrations, as reflected by entries in the `MIGRATIONS` list that migrate several `mz_catalog` and `mz_internal` materialized views including `mz_databases`, `mz_schemas`, `mz_role_members`, `mz_network_policies`, `mz_network_policy_rules`, `mz_cluster_workload_classes`, `mz_internal_cluster_replicas`, `mz_pending_cluster_replicas`, `mz_materialized_views`, `mz_connections`, `mz_secrets`, `mz_sources`, `mz_indexes`, `mz_roles`, and `mz_role_parameters`.
When a builtin's `SystemObjectDescription` changes — for example when a builtin table is converted to a materialized view — existing migration steps naming the old description must be removed regardless of version, and a `Replacement` step for the new description added at the conversion version. `validate_migration_steps` panics on steps that do not resolve to a current builtin.
The `MIGRATIONS` list includes `Replacement` steps at version `26.30.0-dev.0` for `mz_catalog.mz_clusters`, `mz_catalog.mz_cluster_replicas`, `mz_internal.mz_cluster_schedules`, `mz_catalog.mz_default_privileges`, `mz_catalog.mz_system_privileges`, and `mz_catalog.mz_indexes`, reflecting their conversion from `BuiltinTable` to `BuiltinMaterializedView` or SQL fingerprint changes (for `mz_indexes`, the addition of `mz_cluster_replica_size_internal_ind` changed the inlined VALUES set, requiring an explicit replacement step).
A `Replacement` step at version `26.31.0-dev.0` covers `mz_catalog.mz_cluster_replicas`, whose MV definition changed so that the `availability_zone` column aggregates the durable `availability_zones` list rather than a single optional zone.
A `Replacement` step at version `26.32.0-dev.0` covers `mz_internal.mz_comments`, reflecting its conversion from `BuiltinTable` to `BuiltinMaterializedView`.
When applying replacement migrations, `mz_storage_usage_by_shard` is excluded from data-destroying replacement plans to preserve billing data.
When the source and target versions differ and the source version is a dev build, `Migration::run` forces evolution-mode migration even without an explicit `force_migration` config, avoiding version-based filter failures in dev environments.
In forced dev-to-dev migrations, `migrate_evolve_one` skips builtins that do not yet have a shard registered. Brand-new builtins in a given version may not have their shards allocated until the leader completes bootstrap; excluding them avoids "missing shard ID" errors on read-only replicas.
