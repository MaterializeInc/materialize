---
source: src/txn-wal/src/operator.rs
revision: 0a83b723a5
---

# mz-txn-wal::operator

Implements the `txns_progress` Timely dataflow operator, which translates the physical frontier of a data shard into its logical frontier as seen through the txns WAL.
`TxnsContext` holds shared state (a `TxnsRead` handle and shard metadata) passed into the operator closure.
`DataSubscribe` and `DataSubscribeTask` manage an async subscription to a single data shard, driving `TxnsRead` queries to determine when the logical frontier has advanced and emitting remap entries downstream.
The operator is composed of two parts: `txns_progress_source_global` (rendered once via `TxnsProgress::new`) uses `OperatorBuilderRc` to subscribe to the txns shard and emit `DataRemapEntry` updates, while `TxnsProgress::translate` also uses `OperatorBuilderRc` to translate passthrough-stream frontiers using those remap entries, scheduling Tokio tasks to drive progress rather than an async operator loop.
The operator is used by storage readers to correctly report progress on data shards managed by the txn-wal protocol.
