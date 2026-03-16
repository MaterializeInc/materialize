---
source: src/catalog/src/durable/error.rs
revision: 8be2c88c0f
---

# catalog::durable::error

Defines the error hierarchy for durable catalog operations.
`CatalogError` wraps either a `SqlCatalogError` (logical catalog errors) or a `DurableCatalogError` (storage-level errors).
`DurableCatalogError` covers fencing (`FenceError`), incompatible data versions, persist upper mismatches, uninitialized catalog, and general internal errors.
`FenceError` distinguishes being fenced by a higher epoch from being fenced by a newer deploy generation.
