---
source: src/adapter/src/catalog/open.rs
revision: f3b4f3f1be
---

# adapter::catalog::open

Implements `Catalog::open`, the full catalog initialisation sequence run at environment startup.
The function opens the durable catalog store, runs AST migrations (`catalog::migrate`), applies all persisted `StateUpdate` diffs to build the in-memory `CatalogState`, bootstraps built-in objects (schemas, roles, clusters, tables, views, materialized views, functions), runs builtin-schema migrations (see `open::builtin_schema_migration`), and returns a ready `Catalog` along with the initial builtin-table updates to write.
After applying all updates, `state.system_config().sync_dyncfgs()` is called explicitly to mirror the effective `SystemVars` values into the dyncfg `ConfigSet`; `apply_updates` only syncs when a durable `SystemConfiguration` update is present, so a deployment configured purely via `system_parameter_default` would otherwise leave the `ConfigSet` at compile-time defaults for any startup-only read.
`add_new_remove_old_builtin_items_migration` drops comments under every relation-style `CommentObjectId` variant (Table, View, MaterializedView, Source) for a given id, not just the current one. This handles cases where a builtin's type changes (e.g., Table to MaterializedView) but its catalog id is preserved.
`add_new_remove_old_builtin_clusters_migration` emits audit log events when creating or dropping builtin clusters so those operations are visible in `mz_audit_events`.
`reconcile_builtin_cluster_replicas` reconciles each builtin cluster's replica set against the cluster's own durable `replication_factor`, creating missing replicas (allocating IDs via `txn.allocate_system_replica_id()`, single-source and safe because there is no coordinator at that point) and dropping surplus ones. Bootstrap flags seed `size` and `replication_factor` only when a cluster is first created, so an `ALTER CLUSTER` against a builtin cluster persists across restarts. Audit log events are emitted for every create and drop so those operations are visible in `mz_audit_events`.
`remove_pending_cluster_replicas_migration` emits audit log events when dropping pending replicas so the drops are visible in `mz_audit_events`.
