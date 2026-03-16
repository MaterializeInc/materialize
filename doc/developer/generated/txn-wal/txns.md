---
source: src/txn-wal/src/txns.rs
revision: 901d0526a1
---

# mz-txn-wal::txns

Implements `TxnsHandle<K, V, T, D, O, C>`, the primary entry point for interacting with the txns WAL shard.
`register` and `forget` add and remove data shards from the txns protocol; `begin` opens a new `Txn` for writing.
`apply_le(ts)` ensures all committed transactions up to `ts` have been physically applied to their data shards, and `compact_to(ts)` garbage-collects the txns shard up to `ts`.
The handle owns the persist `WriteHandle` for the txns shard and coordinates with `TxnsCache` to track committed-but-unapplied transactions.
