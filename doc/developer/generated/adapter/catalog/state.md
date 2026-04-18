---
source: src/adapter/src/catalog/state.rs
revision: a632912d24
---

# adapter::catalog::state

Defines `CatalogState`, the in-memory representation of the entire Materialize catalog.
`CatalogState` holds `imbl::OrdMap` collections for databases, schemas, roles, role auth (`role_auth_by_id`), clusters, network policies, the item namespace (keyed by both `CatalogItemId` and `GlobalId`), source references, temporary schemas per connection, comments, default privileges, system privileges, system configuration, and a `notices_by_dep_id` index mapping each `GlobalId` to the optimizer notices that depend on it; it also implements `SessionCatalog` for SQL name resolution.
The file also defines `LocalExpressionCache`, a helper used during catalog open and catalog transactions to track which optimizer expressions were found in (or are missing from) the persistent expression cache, avoiding redundant re-optimization.
This is the authoritative read-side view of the catalog; mutations go through `catalog::transact` and are applied by `catalog::apply`.
