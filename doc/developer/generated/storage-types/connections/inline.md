---
source: src/storage-types/src/connections/inline.rs
revision: 5173c50671
---

# storage-types::connections::inline

Provides the traits that support two-phase connection resolution: `ReferencedConnection` (holds a `CatalogItemId`) vs `InlinedConnection` (holds the actual connection details).
`ConnectionResolver` resolves a catalog item ID to an inlined `Connection`; `IntoInlineConnection` recursively substitutes references with their inlined forms.
`ConnectionAccess` is a type-level constraint used across source and sink structs to make them generic over the referenced/inlined distinction. It defines associated types for each supported connection kind: `Kafka`, `Pg`, `Aws`, `Gcp`, `Ssh`, `Csr`, `GlueSchemaRegistry`, `MySql`, `SqlServer`, and `IcebergCatalog`. `ReferencedConnection` maps each to `CatalogItemId`; `InlinedConnection` maps each to the concrete connection struct (e.g. `Gcp` maps to `GcpConnection`).
