---
source: src/storage/src/upsert/memory.rs
revision: 35406f3b86
---

# mz-storage::upsert::memory

Implements `UpsertStateBackend` for `InMemoryHashMap`, a `HashMap<UpsertKey, StateValue>` that tracks its total encoded size.
Used as the in-memory upsert backend for testing and for sources where spill-to-disk is not needed.
