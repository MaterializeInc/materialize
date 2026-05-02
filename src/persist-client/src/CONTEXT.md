# persist-client::src

Root source directory of `mz-persist-client`, the client library for Materialize's persist
durability system. Provides durable time-varying collections (shards) with snapshot-and-subscribe
semantics over parameterized `(K, V, T, D)` codec types.

## Directory layout (LOC ≈ 43,390)

| Path | LOC | Role |
|---|---|---|
| `internal/` | 23,526 | Private state machine, consensus/blob I/O, encoding, metrics, background workers — see [`internal/CONTEXT.md`](internal/CONTEXT.md) |
| `cli/` | ~2,023 | CLI subcommands: `inspect` (read-only dump), `admin` (mutations), `bench` (throughput) |
| `operators/` | ~879 | Timely dataflow `shard_source` operator |
| `lib.rs` | 2,139 | `PersistClient` — entry point; `make_machine`, `open_leased_reader`, `open_writer`, `open_critical_since` |
| `rpc.rs` | 1,883 | gRPC PubSub client/server; `PubSubSender` / `PubSubReceiver` traits + gRPC, in-process, and no-op impls |
| `read.rs` | 1,796 | `ReadHandle`, `Subscribe`, `Listen`, `Cursor` — snapshot and streaming read API |
| `batch.rs` | 1,790 | `Batch`, `BatchBuilder` — incremental write accumulation and pipelined blob upload |
| `fetch.rs` | 1,519 | `LeasedBatchPart`, `FetchedPart`, `EncodedPart`; stats-based pushdown; fetch semaphore |
| `write.rs` | 1,420 | `WriteHandle` — `compare_and_append`, `append`, `batch` / `batch_builder` |
| `iter.rs` | 1,318 | `Consolidator` — streaming merge/consolidation over multiple sorted parts |
| `usage.rs` | 1,261 | Storage utilization introspection |
| `cache.rs` | 1,210 | `PersistClientCache` — process-wide cache of clients, consensus/blob connections, `StateCache`, PubSub task |
| `schema.rs` | 796 | `SchemaCache`, `SchemaCacheMaps`, `CaESchema` — schema evolution and N²-migration caching |
| `cfg.rs` | 747 | `PersistConfig`, all dynamic config knobs via `mz_dyncfg`, retry parameters, version compatibility |
| `critical.rs` | 521 | `SinceHandle` — durable since-frontier holds via `CriticalReaderId` |
| `error.rs` | 228 | `InvalidUsage`, `Since`, `Upper`, error enums |
| `stats.rs` | 168 | Per-part statistics for read-time pushdown |
| `async_runtime.rs` | 115 | `IsolatedRuntime` — Tokio runtime for CPU-bound work (compaction, encoding) |
| `internals_bench.rs` | 51 | Internal microbenchmarks |

## Key concepts

- **`PersistClient`** — constructed from `PersistClientCache::open`; holds `Arc<dyn Blob>`, `Arc<dyn Consensus>`, `Arc<Metrics>`, `Arc<StateCache>`, and `Arc<dyn PubSubSender>`. Creates a `Machine` per shard-open call and hands it to each handle.
- **Handle types** — `WriteHandle`, `ReadHandle`, `SinceHandle` each wrap a `Machine` and expose the public API for their access pattern. Handles heartbeat via background tasks; failure to heartbeat causes lease expiry.
- **`PersistClientCache`** — process-wide singleton. Critical resource: shares Postgres/CRDB consensus connections and blob handles across all clients to stay within connection limits.
- **`PubSubSender` / `PubSubReceiver`** — trait-based seam for state-diff propagation. gRPC implementation for cross-node; in-process delegate for same-node. `rpc.rs` owns implementations; `internal/service.rs` re-exports generated stubs.
- **Decorator stack for `Blob`** — raw blob → `BlobMemCache` (LRU, in `internal/cache.rs`) → `MetricsBlob` (instrumented, in `internal/metrics.rs`). Construction order is set in `PersistClientCache`.
- **Write path** — `WriteHandle::compare_and_append` → `Machine::compare_and_append` → `Applier::apply_unbatched_cmd` → consensus CaS → `StateCache` update + PubSub diff push → `RoutineMaintenance` fan-out.
- **Read path** — `ReadHandle::snapshot` / `Subscribe` → `FetchedPart` via `fetch.rs` → `Consolidator` in `iter.rs`.
- **Compaction** — `WriteHandle` optionally constructs a `Compactor`; after each append `Trace` emits `FueledMergeReq`; `Compactor` processes on `IsolatedRuntime`; result lands back via `Machine::merge_res`.
- **Schema evolution** — `compare_and_evolve_schema` on `Machine`; `SchemaCache` amortizes N²-migration computation per handle.

## Subdirectory bubbling

- `internal/CONTEXT.md` — complete module breakdown of all 18 files, CaS pipeline, pure state transition model, decorator pattern, background work choreography.
- `internal/ARCH_REVIEW.md` — two candidates: (1) `service.rs` placement vs. `rpc.rs`; (2) implicit `Blob` decoration order.

## Cross-references

- Downstream: `mz-txn-wal`, `mz-storage-controller`, `mz-adapter`, `mz-persist-cli`.
- Upstream: `mz-persist` (`Blob`, `Consensus` traits), `mz-persist-types` (`Codec`, `Codec64`), `mz-dyncfg`.
- Generated developer docs: `doc/developer/generated/persist-client/`.
