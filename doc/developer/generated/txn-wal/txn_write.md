---
source: src/txn-wal/src/txn_write.rs
revision: 901d0526a1
---

# mz-txn-wal::txn_write

Defines `Txn` (an in-progress transaction accumulating writes), `TxnWrite` (a single batch of updates for one data shard), and `TxnApply` (the post-commit token used to drive physical application).
`Txn::commit_at(ts)` atomically writes all buffered `TxnWrite` batches plus a txns-shard commit record via compare-and-append; on conflict it retries at a higher timestamp.
`merge` combines two `Txn` objects into one, enabling multi-writer scenarios.
`tidy` appends a cleanup record to the txns shard to retract applied transaction metadata.
