---
source: src/txn-wal/src/txn_read.rs
revision: 901d0526a1
---

# mz-txn-wal::txn_read

Provides the read-side abstractions: `DataSnapshot` (a point-in-time consistent read of one data shard at a logical timestamp), `DataListenNext` (the next event when tailing a data shard), and `DataRemapEntry` (a txns-shard entry mapping a physical batch to a logical timestamp range).
`TxnsRead` is an async handle to a background `TxnsReadTask` that maintains a live `TxnsCache`; callers use it to wait for a data shard's logical frontier to advance past a target timestamp.
`TxnsSubscribeTask` drives a `Subscribe` on the txns shard and feeds updates into `TxnsCache`, keeping it current.
