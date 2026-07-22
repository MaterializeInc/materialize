---
source: src/persist-cli/src/maelstrom.rs
revision: e55821e28d
---

# persistcli::maelstrom

Adapts persist to the Jepsen Maelstrom `txn-list-append` distributed-systems testing framework, providing two workload implementations that exercise persist correctness under network partitions and crash failures.
`node` implements the Maelstrom RPC event loop and `Service` trait; `api` provides the wire types; `services` provides Maelstrom-backed `Blob`, `Consensus`, and timestamp-oracle implementations.
`txn_list_append_single` uses a single persist shard; `txn_list_append_multi` uses the txn-wal multi-shard abstraction.
The `run` entry point sets up the tokio runtime and dispatches to the selected `Service` implementation.
`Args` includes a `--consensus-read-committed` boolean flag that, when set, enables READ COMMITTED isolation for the Postgres consensus backend. The flag defaults to false because it panics when used against CockroachDB, which requires SERIALIZABLE; it must be opted into per consensus backend.
