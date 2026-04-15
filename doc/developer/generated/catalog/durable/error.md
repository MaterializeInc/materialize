---
source: src/catalog/src/durable/error.rs
revision: 00cc513fa5
---

# catalog::durable::error

Defines the error hierarchy for durable catalog operations.
`CatalogError` wraps either a `SqlCatalogError` (logical catalog errors) or a `DurableCatalogError` (storage-level errors).
`DurableCatalogError` covers fencing (`FenceError`), incompatible data versions (`IncompatibleDataVersion`, `IncompatiblePersistVersion`), uninitialized catalog, not-writable catalog, protobuf deserialization errors, duplicate key, uniqueness violations, storage errors, and internal errors. The method `can_recover_with_write_mode` reports whether the error can be recovered by reopening the catalog in writable mode.
`FenceError` has three variants: `DeployGeneration` (fenced by a newer deployment generation), `Epoch` (fenced by a newer epoch), and `MigrationUpper` (fenced during 0dt builtin table migration). The enum is ordered from most to least informative.
