---
source: src/storage-controller/src/persist_handles.rs
revision: 82d92a7fad
---

# storage-controller::persist_handles

Provides `PersistTableWriteWorker<T>`, a cloneable handle to a background Tokio task that serializes all table writes through a txn-wal `TxnsHandle`.
The worker accepts `Register`, `Update`, `DropHandles`, and `Append` commands over an unbounded channel and executes them in order, coordinating timestamp-ordered appends and txns-shard tidying.
The submodule `read_only_table_worker` provides an alternative worker implementation for the read-only mode case that writes directly to persist shards, bypassing txn-wal.
