---
source: src/rocksdb/src/config.rs
revision: 5e4f0eb185
---

# mz-rocksdb::config

Translates `mz-rocksdb-types` configuration types into live `rocksdb::Options` and manages a shared `WriteBufferManager`.

Key types:
* `RocksDBConfig` — the full runtime configuration struct; wraps `RocksDBTuningParameters` from `mz-rocksdb-types` and adds a `RocksDBDynamicConfig` for values (like `batch_size`) that can be updated atomically without restarting the instance.
* `RocksDBDynamicConfig` — holds an `Arc<AtomicUsize>` batch size so cloned configs share live updates.
* `SharedWriteBufferManager` — lazily initializes a single `WriteBufferManager` (backed by a `Weak` pointer) shared across all `RocksDBInstance`s in a process; the manager is released when the last instance drops its `WriteBufferManagerHandle`.
* `apply_to_options` — applies a `RocksDBConfig` to a `rocksdb::Options`, returns an optional `WriteBufferManagerHandle` if write-buffer management is enabled.

Re-exports `defaults` and all types from `mz_rocksdb_types::config`.
