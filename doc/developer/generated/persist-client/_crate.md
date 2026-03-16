---
source: src/persist-client/src/lib.rs
revision: 901d0526a1
---

# persist-client

`mz-persist-client` is the client library for Materialize's persist durability system, providing durable time-varying collections (shards) with snapshot-and-subscribe semantics over arbitrary `(K, V, T, D)` codec types.

The crate root exposes `PersistClient` (created via `PersistClientCache`) as the main entry point, from which callers open `WriteHandle`, `ReadHandle`, and `SinceHandle` for a given shard.

## Module structure

| Module | Role |
|--------|------|
| `lib.rs` / `PersistClient` | Entry point; opens read/write/since handles for a shard |
| `batch` | `Batch` / `BatchBuilder`: write-side accumulation and upload of columnar parts |
| `read` | `ReadHandle` / `Subscribe`: snapshot and streaming read access |
| `write` | `WriteHandle`: compare-and-append write access |
| `critical` | `SinceHandle`: durable since-frontier holds via `CriticalReaderId` |
| `fetch` | Low-level part fetching and decode (used by `read` and `operators`) |
| `iter` | Streaming consolidation of multiple sorted parts |
| `cache` | `PersistClientCache`: process-wide connection and state cache |
| `cfg` | `PersistConfig` and all dynamic configuration knobs |
| `rpc` | gRPC PubSub for propagating state diffs between nodes |
| `schema` | Schema evolution and migration caching |
| `stats` | Per-part statistics for read-time pushdown |
| `usage` | Storage utilization introspection |
| `internal` | Private state machine, compaction, GC, encoding, metrics |
| `operators` | Timely dataflow `shard_source` operator |
| `cli` | CLI subcommands (inspect, admin, bench) |

## Key dependencies

* `mz-persist`: low-level `Blob` and `Consensus` storage abstractions.
* `mz-persist-types`: codec traits (`Codec`, `Codec64`) and columnar format types.
* `differential-dataflow` / `timely`: lattice, antichain, and timestamp types.
* `mz-dyncfg`: runtime-reconfigurable knobs.

## Downstream consumers

`mz-txn-wal`, `mz-storage-controller`, `mz-adapter`, `mz-persist-cli`, and various test crates.
