---
source: src/txn-wal/src/lib.rs
revision: 901d0526a1
---

# mz-txn-wal

Implements atomic multi-shard writes over Materialize's persist layer using a write-ahead log (WAL) stored in a dedicated txns shard.
Writers buffer updates per data shard, then atomically commit a set of batch handles plus a txns-shard record via compare-and-append; readers translate the physical data-shard frontier to a logical one by consulting the txns shard.
The protocol supports two isolation modes: serializable (reads may observe committed data up to the txns-shard frontier) and strict serializable (reads wait for the txns-shard frontier to advance past the requested timestamp).
`TxnsCodecDefault` is the standard codec for txns-shard entries; `small_caa`, `empty_caa`, `apply_caa`, and `cads` are low-level persist helpers used throughout the commit and apply path.

Key types: `TxnsHandle` (write entry point), `Txn` / `TxnApply` / `Tidy` (commit pipeline), `TxnsCache` / `TxnsRead` (read-side cache and async handle), `txns_progress` (Timely operator for frontier translation).

Key dependencies: `mz-persist-client`, `mz-persist-types`, `mz-timely-util`, `differential-dataflow`, `timely`, `prost`.
Downstream consumers: `mz-storage-controller` (registers/commits to data shards), `mz-storage` (reads via `txns_progress` operator).

## Module structure

* `lib.rs` — crate-level protocol documentation, `TxnsCodecDefault`, low-level CaA helpers
* `txns.rs` — `TxnsHandle`: register/forget/begin/apply_le/compact_to
* `txn_write.rs` — `Txn`, `TxnWrite`, `TxnApply`, `commit_at`, `merge`, `tidy`
* `txn_read.rs` — `DataSnapshot`, `DataListenNext`, `DataRemapEntry`, `TxnsRead`, `TxnsReadTask`, `TxnsSubscribeTask`
* `txn_cache.rs` — `TxnsCache`, `TxnsCacheState`: in-memory index of txns shard contents
* `metrics.rs` — `Metrics`, `FallibleOpMetrics`, `InfallibleOpMetrics`, `BatchMetrics`
* `operator.rs` — `txns_progress` Timely operator, `TxnsContext`, `DataSubscribe`, `DataSubscribeTask`
