---
source: src/storage/src/upsert.rs
revision: 5680493e7d
---

# mz-storage::upsert

Implements the upsert timely operator that transforms a stream of key-value updates into a differential collection.
`UpsertKey` is a 32-byte SHA-256 hash of the encoded key row.
The main operator function always delegates to the continual-feedback variant via `upsert_continual_feedback::upsert_inner`; a classic single-pass path exists in the code but is not active.
It manages rehydration from the persist feedback stream, snapshot consolidation, and ongoing updates using pluggable `UpsertStateBackend` implementations (memory or RocksDB).
Submodules `types`, `memory`, and `rocksdb` provide the trait, in-memory, and disk-backed state implementations.
