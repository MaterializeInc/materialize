---
source: src/storage/src/upsert/types.rs
revision: 64377faf17
---

# mz-storage::upsert::types

Defines the `UpsertStateBackend` trait and the `UpsertState` wrapper that drives UPSERT key-value state management.
`UpsertStateBackend` requires async `multi_get` and `multi_put` operations and optionally `multi_merge` (gated by `supports_merge()`), with size reporting via `PutStats`, `GetStats`, and `MergeStats`.
`UpsertState` adds higher-level APIs including `consolidate_chunk` for re-indexing collections using the output differential collection.
Also defines `StateValue`, `Value`, `ProvisionalValue`, `Consolidating`, `ValueMetadata`, `PutValue`, `MergeValue`, `SnapshotStats`, and the bincode serialization options used for persisting state.
`FuzzUpsertParts` (behind `#[cfg(feature = "fuzzing")]`) owns the metrics and statistics plumbing needed by `UpsertState`, allowing fuzz targets to build fresh in-memory `UpsertState` instances without reconstructing metrics on each iteration. `StateValue::memory_size` is gated on `#[cfg(any(test, feature = "fuzzing"))]`.
