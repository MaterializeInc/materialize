---
source: src/adapter/src/catalog.rs
revision: af5783aa4f
---

# adapter::catalog

The coordinator's catalog layer: wraps the `mz_catalog` crate's durable store and in-memory objects with the additional adapter-specific logic needed to drive DDL and serve catalog queries.
`Catalog` is the top-level struct exposing the full catalog API: it holds a `CatalogState` (in-memory view), an `ExpressionCacheHandle`, and delegates persistence to the durable store.
Child modules divide responsibilities: `state` owns the in-memory data structures and `SessionCatalog` impl; `transact` executes atomic DDL operations; `apply` reconciles durable diffs into in-memory state; `open` bootstraps the catalog at startup; `migrate` rewrites stored SQL for syntax changes; `builtin_table_updates` maintains system-table row diffs; `consistency` provides invariant checking; and `timeline` resolves timeline contexts for timestamp selection.
