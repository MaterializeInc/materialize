---
source: src/rocksdb/src/lib.rs
revision: 7ffd40e1ab
---

# mz-rocksdb

Provides an async wrapper around RocksDB that executes all blocking I/O on a dedicated background thread, exposing a channel-based API optimized for batched operations.

The primary type is `RocksDBInstance<K, V>`, which spawns a `rocksdb_core_loop` thread on construction.
Callers communicate with the thread via `mpsc` channels using the internal `Command` enum (`MultiGet`, `MultiUpdate`, `Shutdown`, `ManualCompaction`).
The public API consists of `multi_get` (batch key lookup), `multi_update` (batch put/merge/delete via `KeyUpdate`), `manual_compaction`, and `close`.
Batches are split into chunks sized by the dynamic `batch_size` config, and individual RocksDB errors that are `TryAgain` are retried up to `retry_max_duration`.

Key types:
* `InstanceOptions<O, V, F>` — fixed startup options including a possibly shared RocksDB `Env`, bincode serializer, WAL flag, optional merge operator, and cleanup behavior. Cleanup routes `DB::destroy` through the instance `Env` so that in-memory environments (used by replicas without a scratch directory) are properly cleaned.
* `RocksDBSharedMetrics` / `RocksDBInstanceMetrics` — user-supplied Prometheus metrics for latency, throughput, and batch sizes.
* `ValueIterator` — iterates over existing and new operand values during a merge operation.
* `Error` — covers RocksDB errors, decode failures, thread shutdown, and cleanup timeouts.

Modules:
* `config` — translates `mz-rocksdb-types` tuning parameters into `rocksdb::Options` and manages a shared `WriteBufferManager`.

Key dependencies: `rocksdb`, `mz-rocksdb-types`, `mz-ore`, `bincode`, `tokio`.
Used by storage-layer components (e.g., upsert operators) that need durable, high-throughput key-value storage backed by RocksDB.
