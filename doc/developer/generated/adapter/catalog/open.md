---
source: src/adapter/src/catalog/open.rs
revision: fca741734d
---

# adapter::catalog::open

Implements `Catalog::open`, the full catalog initialisation sequence run at environment startup.
The function opens the durable catalog store, runs AST migrations (`catalog::migrate`), applies all persisted `StateUpdate` diffs to build the in-memory `CatalogState`, bootstraps built-in objects (schemas, roles, clusters, tables, views, materialized views, functions), runs builtin-schema migrations (see `open::builtin_schema_migration`), and returns a ready `Catalog` along with the initial builtin-table updates to write.
`add_new_remove_old_builtin_items_migration` drops comments under every relation-style `CommentObjectId` variant (Table, View, MaterializedView, Source) for a given id, not just the current one. This handles cases where a builtin's type changes (e.g., Table to MaterializedView) but its catalog id is preserved.
`add_new_remove_old_builtin_clusters_migration` emits audit log events when creating or dropping builtin clusters so those operations are visible in `mz_audit_events`.
`add_new_remove_old_builtin_cluster_replicas_migration` allocates builtin replica IDs via `txn.allocate_system_replica_id()` within the open transaction (single-source, safe because there is no coordinator at that point) and emits audit log events when creating or dropping builtin cluster replicas so those operations are visible in `mz_audit_events`.
`remove_pending_cluster_replicas_migration` emits audit log events when dropping pending replicas so the drops are visible in `mz_audit_events`.
