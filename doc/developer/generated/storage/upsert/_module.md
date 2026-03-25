---
source: src/storage/src/upsert.rs
revision: e1e5d200de
---

# mz-storage::upsert

Implements the upsert timely operator that transforms a stream of key-value updates into a differential collection.
`UpsertKey` is a 32-byte SHA-256 hash of the encoded key row.
The main operator function creates either a traditional single-pass upsert or, for spill-to-disk configurations, a continual-feedback variant via `upsert_continual_feedback`.
It manages rehydration from the persist feedback stream, snapshot consolidation, and ongoing updates using pluggable `UpsertStateBackend` implementations (memory or RocksDB).
Submodules `types`, `memory`, and `rocksdb` provide the trait, in-memory, and disk-backed state implementations.
