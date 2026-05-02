---
source: src/sql/src/catalog.rs
revision: 9d0a7c3c6f
---

# mz-sql::catalog

Defines the `SessionCatalog` trait — the abstraction layer between the SQL planner and any concrete catalog implementation.
The trait covers resolution (converting partial names to fully-qualified names), lookup (retrieving metadata about databases, schemas, items, roles, clusters, etc.), and session management (variables, notices, privilege queries).
Supporting types such as `CatalogItem`, `CatalogItemType` (variants: `Table`, `Source`, `Sink`, `View`, `MaterializedView`, `Index`, `Type`, `Func`, `Secret`, `Connection`), `CatalogDatabase`, `CatalogSchema`, `CatalogCluster`, `CatalogRole`, `CatalogType`, and `CatalogError` are all defined here, making this the central interface contract for the entire SQL layer.
`ObjectType` covers the full set of catalog object kinds visible in SQL; `CatalogConfig` carries session-level configuration including `connection_context` and optional `helm_chart_version`.
Also defines `AutoProvisionSource` (enum tracking whether a role was auto-provisioned by OIDC, Frontegg, or None), `RoleAttributes` and `RoleAttributesRaw` (which include an `auto_provision_source` field), and `PasswordAction`.
