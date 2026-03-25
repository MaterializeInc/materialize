---
source: src/txn-wal/src/txn_cache.rs
revision: a55caae279
---

# mz-txn-wal::txn_cache

Implements `TxnsCache` and `TxnsCacheState`, an in-memory index of the txns shard contents for efficient read-path queries.
`TxnsCacheState` maps each registered data shard to its ordered list of `DataRemapEntry` records (committed batches with their logical timestamp intervals) and tracks the overall txns shard frontier.
`TxnsCache` wraps `TxnsCacheState` with a `Subscribe` handle to the txns shard, driving incremental updates as new transactions commit and old ones are tidied.
Queries like `data_snapshot(shard, ts)` and `data_listen_next(shard, ts)` consult the cache to translate physical persist events into logical timestamps without re-reading the txns shard.
