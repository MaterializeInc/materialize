---
source: src/adapter/src/catalog/open.rs
revision: a632912d24
---

# adapter::catalog::open

Implements `Catalog::open`, the full catalog initialisation sequence run at environment startup.
The function opens the durable catalog store, runs AST migrations (`catalog::migrate`), applies all persisted `StateUpdate` diffs to build the in-memory `CatalogState`, bootstraps built-in objects (schemas, roles, clusters, tables, views, materialized views, functions), runs builtin-schema migrations (see `open::builtin_schema_migration`), and returns a ready `Catalog` along with the initial builtin-table updates to write.
`add_new_remove_old_builtin_items_migration` handles `Builtin::MaterializedView` for column comments.
`add_new_remove_old_builtin_clusters_migration` emits audit log events when creating or dropping builtin clusters so those operations are visible in `mz_audit_events`.
`add_new_remove_old_builtin_cluster_replicas_migration` emits audit log events when creating or dropping builtin cluster replicas so those operations are visible in `mz_audit_events`.
`remove_pending_cluster_replicas_migration` emits audit log events when dropping pending replicas so the drops are visible in `mz_audit_events`.
