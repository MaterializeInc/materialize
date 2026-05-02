# mz_txn_wal/src

Source root of `mz-txn-wal`. Cargo-conventional location.

See [`../CONTEXT.md`](../CONTEXT.md) for the crate's role: cross-shard
atomicity substrate over persist; one-shard linearization is by design;
`txns_progress` Timely operator is mandatory for any reader.
