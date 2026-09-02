---
source: src/storage-controller/src/persist_handles.rs
revision: a60edac7f1
---

# storage-controller::persist_handles

Provides `PersistTableWriteWorker`, a cloneable handle to a background Tokio task that serializes all table writes through a txn-wal `TxnsHandle`.
The worker accepts `Register`, `DropHandles`, `Append`, and `Shutdown` commands over an unbounded channel and executes them in order, coordinating timestamp-ordered appends and txns-shard tidying.
`PersistTableWriteWorker::append` always forwards the command to the txn-wal layer, including for empty-update calls; the txn-wal commit advances the logical upper of all registered data shards, which is required for periodic group commits that carry no actual data writes.
On a successful commit, the caller is blocked until the apply completes. The txns-shard commit makes the write durable and linearized; the apply then happens synchronously before the caller is unblocked.
The submodule `read_only_table_worker` provides an alternative worker implementation for the read-only mode case that writes directly to persist shards, bypassing txn-wal.
