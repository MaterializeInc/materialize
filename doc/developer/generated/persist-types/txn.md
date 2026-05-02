---
source: src/persist-types/src/txn.rs
revision: 4a94b2e498
---

# persist-types::txn

Defines `TxnsEntry` (the in-memory representation of an entry in the txns shard — either a `Register` or an `Append` with an encoded timestamp) and the `TxnsCodec` trait that abstracts over how `TxnsEntry` values are serialized into the txns shard.
`TxnsCodec` also exposes a `should_fetch_part` method for pushdown filtering of txns shard parts by `ShardId`.
