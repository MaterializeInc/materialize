---
source: src/adapter/src/catalog/open.rs
revision: 2c413b395c
---

# adapter::catalog::open

Implements `Catalog::open`, the full catalog initialisation sequence run at environment startup.
The function opens the durable catalog store, runs AST migrations (`catalog::migrate`), applies all persisted `StateUpdate` diffs to build the in-memory `CatalogState`, bootstraps built-in objects (schemas, roles, clusters, tables, views, functions), runs builtin-schema migrations (see `open::builtin_schema_migration`), and returns a ready `Catalog` along with the initial builtin-table updates to write.
