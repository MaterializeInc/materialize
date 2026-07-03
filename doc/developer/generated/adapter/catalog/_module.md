---
source: src/adapter/src/catalog.rs
revision: e926ec3a86
---

# adapter::catalog

The coordinator's catalog layer: wraps the `mz_catalog` crate's durable store and in-memory objects with the additional adapter-specific logic needed to drive DDL and serve catalog queries.
`Catalog` is the top-level struct exposing the full catalog API: it holds a `CatalogState` (in-memory view), an `ExpressionCacheHandle`, and delegates persistence to the durable store.
Child modules divide responsibilities: `state` owns the in-memory data structures and `SessionCatalog` impl; `transact` executes atomic DDL operations; `apply` reconciles durable diffs into in-memory state; `open` bootstraps the catalog at startup; `migrate` rewrites stored SQL for syntax changes; `builtin_table_updates` maintains system-table row diffs; `consistency` provides invariant checking; `timeline` resolves timeline contexts for timestamp selection; and `cluster_state` projects live cluster config into plain-data `ExpectedClusterState` structs for conditional catalog writes.
Re-exports include `InjectedAuditEvent` from `transact`. Builtin materialized views are handled alongside tables and views in descriptor tests.
`Catalog::allocate_storage_usage_id(commit_ts)` is a thin wrapper around `DurableCatalogState::allocate_id` that bumps the `STORAGE_USAGE_ID_ALLOC_KEY` allocator by one and returns the allocated id, committed at `commit_ts`. It bypasses the high-level catalog transaction machinery; one id covers all rows produced in a single collection cycle.
`Catalog::allocate_user_replica_ids`, `allocate_system_replica_ids`, and `allocate_replica_ids` delegate to the durable store to pre-allocate replica IDs out-of-band before a catalog transaction. `allocate_replica_ids` dispatches to user or system allocation based on the owning cluster's ID type.
