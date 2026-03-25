---
source: src/storage/src/upsert/rocksdb.rs
revision: e757b4d11b
---

# mz-storage::upsert::rocksdb

Implements `UpsertStateBackend` for `RocksDB<T, O>`, wrapping a `RocksDBInstance<UpsertKey, StateValue<T, O>>` to persist upsert state to disk.
Translates `multi_put` calls into `KeyUpdate::Put`/`Delete`/`Merge` operations and `multi_get` calls into batch reads, delegating to the underlying RocksDB crate for actual I/O.
