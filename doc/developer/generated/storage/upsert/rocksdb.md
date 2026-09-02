---
source: src/storage/src/upsert/rocksdb.rs
revision: 7ffd40e1ab
---

# mz-storage::upsert::rocksdb

Implements `UpsertStateBackend` for `RocksDB<T, O>`, wrapping a `RocksDBInstance<UpsertKey, StateValue<T, O>>` to persist upsert state to disk.
Translates `multi_put` calls into `KeyUpdate::Put`/`Delete`/`Merge` operations and `multi_get` calls into batch reads, delegating to the underlying RocksDB crate for actual I/O.
