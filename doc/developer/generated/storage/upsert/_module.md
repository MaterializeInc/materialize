---
source: src/storage/src/upsert.rs
revision: 844b947128
---

# mz-storage::upsert

Implements the upsert timely operator that transforms a stream of key-value updates into a differential collection.
`UpsertKey` is a 32-byte SHA-256 hash of the encoded key row.
The `upsert` function delegates to the continual-feedback variant via `upsert_continual_feedback::upsert_inner`, managing rehydration from the persist feedback stream, snapshot consolidation, and ongoing updates using pluggable `UpsertStateBackend` implementations (memory or RocksDB).
The `upsert_v2` function provides an alternative implementation that uses a differential dataflow collection to hold key state, delegating to `upsert_continual_feedback_v2::upsert_inner`; it is selected at render time via the `ENABLE_UPSERT_V2` dyncfg. It accepts a `UpsertStashFlavor` resolved at operator construction time.
The `upsert_stash_spill` public module exposes `set_enabled(bool)`, which sets the storage leg of the process-wide chunk spill gate used by the chunked upsert-v2 stash flavor. Chunks spill to the process buffer pool while either the storage leg or compute's leg is set; storage controls only its own leg.
The `upsert_stash_pager` public module exposes `set_enabled(bool)`, which controls the storage-owned column pager used by the paged upsert-v2 stash flavor. The pager draws from the same process-wide budget as the compute column-paged batcher; storage decides independently whether its stash participates. Flipping the flag takes effect on dataflows created after the change.
Submodules `types`, `memory`, and `rocksdb` provide the trait, in-memory, and disk-backed state implementations. The `memory` submodule is gated on `#[cfg(any(test, feature = "fuzzing"))]`, making it accessible to the storage fuzz crate in addition to tests.
