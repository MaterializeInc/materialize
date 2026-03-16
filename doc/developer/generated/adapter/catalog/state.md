---
source: src/adapter/src/catalog/state.rs
revision: 28fcb17b18
---

# adapter::catalog::state

Defines `CatalogState`, the in-memory representation of the entire Materialize catalog.
`CatalogState` holds BTreeMaps for databases, schemas, roles, clusters, network policies, the item namespace (keyed by both `CatalogItemId` and `GlobalId`), temporary objects per connection, comments, default privileges, audit log events, and system configuration; it also implements `SessionCatalog` for SQL name resolution.
This is the authoritative read-side view of the catalog; mutations go through `catalog::transact` and are applied by `catalog::apply`.
