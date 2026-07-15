---
source: src/sql/src/catalog.rs
revision: bfdf7c6abb
---

# mz-sql::catalog

Defines the `SessionCatalog` trait — the abstraction layer between the SQL planner and any concrete catalog implementation.
The trait covers resolution (converting partial names to fully-qualified names), lookup (retrieving metadata about databases, schemas, items, roles, clusters, etc.), and session management (variables, notices, privilege queries, and access-restriction flags such as `restrict_to_user_objects`).
Supporting types such as `CatalogItem`, `CatalogItemType` (variants: `Table`, `Source`, `Sink`, `View`, `MaterializedView`, `Index`, `Type`, `Func`, `Secret`, `Connection`), `CatalogDatabase`, `CatalogSchema`, `CatalogCluster`, `CatalogRole`, `CatalogType`, and `CatalogError` are all defined here, making this the central interface contract for the entire SQL layer.
`ObjectType` covers the full set of catalog object kinds visible in SQL; `CatalogConfig` carries session-level configuration including `connection_context` and optional `helm_chart_version`.
Also defines `AutoProvisionSource` (enum tracking whether a role was auto-provisioned by OIDC, Frontegg, or None), `RoleAttributes` and `RoleAttributesRaw` (which include an `auto_provision_source` field), and `PasswordAction`.
`CatalogType::Record::to_relation_desc` allocates a shared `TypeResolutionBudget` (via `query::TypeResolutionBudget::for_root`) before iterating over record fields, and resolves each field's type through `budget.resolve_child` instead of a standalone `scalar_type_from_catalog` call. This ensures all fields of a record type share one resolution budget, preventing a wide record whose fields are individually within the bound from materializing an unbounded type tree.
