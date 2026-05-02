---
source: src/storage/src/upsert/types.rs
revision: 5680493e7d
---

# mz-storage::upsert::types

Defines the `UpsertStateBackend` trait and the `UpsertState` wrapper that drives UPSERT key-value state management.
`UpsertStateBackend` requires async `multi_get` and `multi_put` operations and optionally `multi_merge` (gated by `supports_merge()`), with size reporting via `PutStats`, `GetStats`, and `MergeStats`.
`UpsertState` adds higher-level APIs including `consolidate_chunk` for re-indexing collections using the output differential collection.
Also defines `StateValue`, `Value`, `ProvisionalValue`, `Consolidating`, `ValueMetadata`, `PutValue`, `MergeValue`, `SnapshotStats`, and the bincode serialization options used for persisting state.
