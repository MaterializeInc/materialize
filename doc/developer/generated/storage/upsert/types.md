---
source: src/storage/src/upsert/types.rs
revision: e1e5d200de
---

# mz-storage::upsert::types

Defines the `UpsertStateBackend` trait and the `UpsertState` wrapper that drives UPSERT key-value state management.
`UpsertStateBackend` requires async `multi_get` and `multi_put` operations (optionally `merge`), with chunk-based batching and size reporting.
`UpsertState` adds higher-level APIs including `consolidate_chunk` for re-indexing collections using the output differential collection.
Also defines `StateValue`, `ValueMetadata`, `PutValue`, `PutStats`, `GetStats`, `MergeStats`, and the bincode serialization options used for persisting state.
