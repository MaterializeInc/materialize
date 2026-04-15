---
source: src/txn-wal/src/operator.rs
revision: b0fa98e931
---

# mz-txn-wal::operator

Implements the `txns_progress` Timely dataflow operator, which translates the physical frontier of a data shard into its logical frontier as seen through the txns WAL.
`TxnsContext` holds shared state (a `TxnsRead` handle and shard metadata) passed into the operator closure.
`DataSubscribe` and `DataSubscribeTask` manage an async subscription to a single data shard, driving `TxnsRead` queries to determine when the logical frontier has advanced and emitting remap entries downstream.
The operator is used by storage readers to correctly report progress on data shards managed by the txn-wal protocol.
