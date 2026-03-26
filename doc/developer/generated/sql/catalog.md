---
source: src/sql/src/catalog.rs
revision: aa7a1afd31
---

# mz-sql::catalog

Defines the `SessionCatalog` trait — the abstraction layer between the SQL planner and any concrete catalog implementation.
The trait covers resolution (converting partial names to fully-qualified names), lookup (retrieving metadata about databases, schemas, items, roles, clusters, etc.), and session management (variables, notices, privilege queries).
Supporting types such as `CatalogItem`, `CatalogItemType`, `CatalogDatabase`, `CatalogSchema`, `CatalogCluster`, `CatalogRole`, `CatalogType`, and `CatalogError` are all defined here, making this the central interface contract for the entire SQL layer.
Also defines `AutoProvisionSource` (enum tracking whether a role was auto-provisioned by OIDC, Frontegg, or None), `RoleAttributes` and `RoleAttributesRaw` (which include an `auto_provision_source` field), and `PasswordAction`.
