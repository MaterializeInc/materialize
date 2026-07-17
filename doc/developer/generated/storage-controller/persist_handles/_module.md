---
source: src/storage-controller/src/persist_handles.rs
revision: 6c590b0a3c
---

# storage-controller::persist_handles

Provides `PersistTableWriteWorker`, a cloneable handle to a background Tokio task that serializes all table writes through a txn-wal `TxnsHandle`.
The worker accepts `Register`, `Update`, `DropHandles`, and `Append` commands over an unbounded channel and executes them in order, coordinating timestamp-ordered appends and txns-shard tidying.
`PersistTableWriteWorker::append` always forwards the command to the txn-wal layer, including for empty-update calls; the txn-wal commit advances the logical upper of all registered data shards, which is required for periodic group commits that carry no actual data writes.
On a successful commit, the caller is unblocked before the txn-wal apply step: because the commit to the txns shard already makes the write durable and linearized, deferring the apply shifts latency from writes onto reads of the just-written shards. The apply is idempotent and all reads of the affected data shards block until it completes. Apply latency is tracked via an `apply_seconds` histogram passed to `new_txns`.
The submodule `read_only_table_worker` provides an alternative worker implementation for the read-only mode case that writes directly to persist shards, bypassing txn-wal.
