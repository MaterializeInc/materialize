---
source: src/sql/src/catalog.rs
revision: f2656c001e
---

# mz-sql::catalog

Defines the `SessionCatalog` trait — the abstraction layer between the SQL planner and any concrete catalog implementation.
The trait covers resolution (converting partial names to fully-qualified names), lookup (retrieving metadata about databases, schemas, items, roles, clusters, etc.), and session management (variables, notices, privilege queries).
Supporting types such as `CatalogItem`, `CatalogItemType`, `CatalogDatabase`, `CatalogSchema`, `CatalogCluster`, `CatalogRole`, `CatalogType`, and `CatalogError` are all defined here, making this the central interface contract for the entire SQL layer.
