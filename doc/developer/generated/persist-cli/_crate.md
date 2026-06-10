---
source: src/persist-cli/src/main.rs
revision: 82d92a7fad
---

# persistcli

A command-line binary (`persistcli`) that bundles developer and testing utilities for the persist storage layer.
It exposes six subcommands: `maelstrom` and `maelstrom-txn` (Jepsen Maelstrom workload adapters for single-shard and txn-wal multi-shard modes), `open-loop` (throughput benchmark), `inspect` and `admin` (delegated to `mz-persist-client` CLI modules for shard inspection and administration), `bench` (persist microbenchmarks), and `service` (PubSub transport smoke-test).

## Module structure

* `main.rs` — CLI argument parsing and subcommand dispatch.
* `maelstrom` — Maelstrom workload driver and `Service` trait; contains `api`, `node`, `services`, `txn_list_append_single`, and `txn_list_append_multi` submodules.
* `open_loop` — Open-loop throughput benchmark.
* `service` — PubSub server/writer/reader test harness.

## Key dependencies

`mz-persist-client` (core persist API and CLI modules), `mz-persist` (low-level blob/consensus and `DataGenerator`), `mz-txn-wal` (multi-shard txn abstraction), `mz-timestamp-oracle` (timestamp oracle implementations), `mz-ore`, `clap`.
