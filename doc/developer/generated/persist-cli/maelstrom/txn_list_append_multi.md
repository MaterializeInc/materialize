---
source: src/persist-cli/src/maelstrom/txn_list_append_multi.rs
revision: e55821e28d
---

# persistcli::maelstrom::txn_list_append_multi

Implements the Maelstrom `txn-list-append` workload using the multi-shard `TxnsHandle` abstraction from `mz-txn-wal`, where each logical key maps to a distinct persist shard (derived by hashing the key and the txns-shard ID).
`Transactor` coordinates reads via `DataSubscribeTask` subscriptions (to recover per-item commit timestamps needed for list ordering) and commits writes through `TxnsHandle::begin`/`commit_at`, retrying at higher timestamps on conflict.
`TransactorService` implements `Service` and wires the transactor to Maelstrom or external blob/consensus backends and either `PostgresTimestampOracle` or `MemTimestampOracle`.
This variant provides heavier coverage of the txn-wal code paths compared to `txn_list_append_single`.
READ COMMITTED isolation for the Postgres consensus backend is enabled only when `args.consensus_read_committed` is set, rather than unconditionally. Unconditional enablement panics against CockroachDB, which requires SERIALIZABLE.
