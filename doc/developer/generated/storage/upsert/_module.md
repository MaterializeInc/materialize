---
source: src/storage/src/upsert.rs
revision: 3da45d073d
---

# mz-storage::upsert

Implements the upsert timely operator that transforms a stream of key-value updates into a differential collection.
`UpsertKey` is a 32-byte SHA-256 hash of the encoded key row.
The `upsert` function delegates to the continual-feedback variant via `upsert_continual_feedback::upsert_inner`, managing rehydration from the persist feedback stream, snapshot consolidation, and ongoing updates using pluggable `UpsertStateBackend` implementations (memory or RocksDB).
The `upsert_v2` function provides an alternative implementation that uses a differential dataflow collection to hold key state, delegating to `upsert_continual_feedback_v2::upsert_inner`; it is selected at render time via the `ENABLE_UPSERT_V2` dyncfg.
Submodules `types`, `memory`, and `rocksdb` provide the trait, in-memory, and disk-backed state implementations.
