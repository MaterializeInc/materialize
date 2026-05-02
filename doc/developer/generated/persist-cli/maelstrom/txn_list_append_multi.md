---
source: src/persist-cli/src/maelstrom/txn_list_append_multi.rs
revision: b89a9e0ec5
---

# persistcli::maelstrom::txn_list_append_multi

Implements the Maelstrom `txn-list-append` workload using the multi-shard `TxnsHandle` abstraction from `mz-txn-wal`, where each logical key maps to a distinct persist shard (derived by hashing the key and the txns-shard ID).
`Transactor` coordinates reads via `DataSubscribeTask` subscriptions (to recover per-item commit timestamps needed for list ordering) and commits writes through `TxnsHandle::begin`/`commit_at`, retrying at higher timestamps on conflict.
`TransactorService` implements `Service` and wires the transactor to Maelstrom or external blob/consensus backends and either `PostgresTimestampOracle` or `MemTimestampOracle`.
This variant provides heavier coverage of the txn-wal code paths compared to `txn_list_append_single`.
