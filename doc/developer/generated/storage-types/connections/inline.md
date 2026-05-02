---
source: src/storage-types/src/connections/inline.rs
revision: a375623c5b
---

# storage-types::connections::inline

Provides the traits that support two-phase connection resolution: `ReferencedConnection` (holds a `CatalogItemId`) vs `InlinedConnection` (holds the actual connection details).
`ConnectionResolver` resolves a catalog item ID to an inlined `Connection`; `IntoInlineConnection` recursively substitutes references with their inlined forms.
`ConnectionAccess` is a type-level constraint used across source and sink structs to make them generic over the referenced/inlined distinction.
