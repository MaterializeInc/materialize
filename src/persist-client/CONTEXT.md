# persist-client

`mz-persist-client` — client library for Materialize's persist durability system.
Provides durable time-varying collections (shards) with snapshot-and-subscribe
semantics over parameterized `(K, V, T, D)` codec types.

Primary intended consumers: `mz-storage-controller`, `mz-txn-wal`, `mz-adapter`.
Code outside storage should not call persist directly; the storage layer mediates.

## Crate layout (LOC ≈ 44,363)

| Path | LOC | Role |
|---|---|---|
| `src/internal/` | 23,526 | Private state machine, consensus/blob I/O, encoding, metrics, background workers — see [`src/internal/CONTEXT.md`](src/internal/CONTEXT.md) |
| `src/cli/` | ~2,023 | CLI subcommands: `inspect` (read-only dump), `admin` (mutations), `bench` (throughput) |
| `src/operators/` | ~879 | Timely dataflow `shard_source` operator |
| `src/lib.rs` | 2,139 | `PersistClient` — entry point; `make_machine`, `open_leased_reader`, `open_writer`, `open_critical_since` |
| `src/rpc.rs` | 1,883 | gRPC PubSub client/server; `PubSubSender` / `PubSubReceiver` traits + gRPC, in-process, and no-op impls |
| `src/read.rs` | 1,796 | `ReadHandle`, `Subscribe`, `Listen`, `Cursor` — snapshot and streaming read API |
| `src/batch.rs` | 1,790 | `Batch`, `BatchBuilder` — incremental write accumulation and pipelined blob upload |
| `src/fetch.rs` | 1,519 | `LeasedBatchPart`, `FetchedPart`, `EncodedPart`; stats-based pushdown; fetch semaphore |
| `src/write.rs` | 1,420 | `WriteHandle` — `compare_and_append`, `append`, `batch` / `batch_builder` |
| `src/iter.rs` | 1,318 | `Consolidator` — streaming merge/consolidation over multiple sorted parts |
| `src/usage.rs` | 1,261 | Storage utilization introspection |
| `src/cache.rs` | 1,210 | `PersistClientCache` — process-wide cache of clients, consensus/blob connections, `StateCache`, PubSub task |
| `src/schema.rs` | 796 | `SchemaCache`, `SchemaCacheMaps`, `CaESchema` — schema evolution and N²-migration caching |
| `src/cfg.rs` | 747 | `PersistConfig`, all dynamic config knobs via `mz_dyncfg`, retry parameters, version compatibility |
| `src/critical.rs` | 521 | `SinceHandle` — durable since-frontier holds via `CriticalReaderId` |
| `src/error.rs` | 228 | `InvalidUsage`, `Since`, `Upper`, error enums |
| `src/stats.rs` | 168 | Per-part statistics for read-time pushdown |
| `src/async_runtime.rs` | 115 | `IsolatedRuntime` — Tokio runtime for CPU-bound work (compaction, encoding) |
| `benches/` | ~879 | Criterion benchmarks (`benches.rs`, `porcelain.rs`, `plumbing.rs`) |
| `tests/` | ~0 | Integration test entry point |
| `build.rs` | — | Proto codegen for PubSub gRPC stubs |

## Key concepts (client layer)

- **`PersistClient`** — constructed from `PersistClientCache::open`; holds `Arc<dyn Blob>`, `Arc<dyn Consensus>`, `Arc<Metrics>`, `Arc<StateCache>`, and `Arc<dyn PubSubSender>`. Creates a `Machine` per shard-open call and hands it to each handle.
- **Handle types** — `WriteHandle`, `ReadHandle`, `SinceHandle` each wrap a `Machine` and expose the public API for their access pattern. Handles heartbeat via background tasks; failure to heartbeat causes lease expiry.
- **`PersistClientCache`** — process-wide singleton. Critical resource: shares Postgres/CRDB consensus connections and blob handles across all clients to stay within connection limits.
- **`PubSubSender` / `PubSubReceiver`** — trait-based seam for state-diff propagation. gRPC implementation for cross-node; in-process delegate for same-node. `rpc.rs` owns implementations; `internal/service.rs` re-exports generated stubs.
- **Decorator stack for `Blob`** — raw blob → `BlobMemCache` (LRU, in `internal/cache.rs`) → `MetricsBlob` (instrumented, in `internal/metrics.rs`). Construction order is set in `PersistClientCache`.
- **Write path** — `WriteHandle::compare_and_append` → `Machine::compare_and_append` → `Applier::apply_unbatched_cmd` → consensus CaS → `StateCache` update + PubSub diff push → `RoutineMaintenance` fan-out.
- **Read path** — `ReadHandle::snapshot` / `Subscribe` → `FetchedPart` via `fetch.rs` → `Consolidator` in `iter.rs`.
- **Compaction** — `WriteHandle` optionally constructs a `Compactor`; after each append `Trace` emits `FueledMergeReq`; `Compactor` processes on `IsolatedRuntime`; result lands back via `Machine::merge_res`.
- **Schema evolution** — `compare_and_evolve_schema` on `Machine`; `SchemaCache` amortizes N²-migration computation per handle.

## System summary

Persist is the durability substrate for STORAGE. Its core abstraction is the **shard** — a
durable, definite Time-Varying Collection parameterized by `(K, V, T, D)` codec types.
Persist is deliberately independent of Materialize's internal data formats (`Row`, etc.).

### Public API surface

| Type | File | Access pattern |
|---|---|---|
| `PersistClientCache` | `src/cache.rs` | Process-wide constructor; open a `PersistClient` per `PersistLocation` |
| `PersistClient` | `src/lib.rs` | Open `WriteHandle`, `ReadHandle`, `SinceHandle` per shard |
| `WriteHandle` | `src/write.rs` | `compare_and_append`, `append`, `batch` |
| `ReadHandle` | `src/read.rs` | `snapshot`, `listen`, `subscribe` |
| `SinceHandle` | `src/critical.rs` | Durable since-frontier hold |
| `BatchBuilder` / `Batch` | `src/batch.rs` | Staged writes (build then append) |
| `shard_source` | `src/operators/` | Timely dataflow source operator |

### Storage abstractions (from `mz-persist`)

- `Blob` — object store (S3, GCS, in-mem). Decorated in order: raw → `BlobMemCache` (LRU) → `MetricsBlob` (instrumented).
- `Consensus` — linearizable CaS store (CRDB/Postgres). Decorated: raw → `MetricsConsensus`.

### Internal architecture (from `src/internal/`)

Write path: `WriteHandle` → `Machine` (retry loop) → `Applier` (CaS seam) → `StateVersions` (consensus) → `StateCache` update + PubSub diff push → `RoutineMaintenance` fan-out to `GarbageCollector`, rollup writer, `Compactor`.

State model: `TypedState` / `StateCollections` — all transitions are pure functions; `Machine` retries on CaS mismatch. Compaction is async: `Trace` (Spine fork) emits `FueledMergeReq`; `Compactor` runs on `IsolatedRuntime`.

### Configuration

`PersistConfig` in `src/cfg.rs` — all knobs via `mz_dyncfg::Config`, runtime-reconfigurable via `ConfigUpdates::apply_from`. Covers blob target size, compaction heuristics, retry parameters, writer lease duration, connection pool limits, and version-compatibility gates.

## Subdirectory bubbling

- `src/internal/CONTEXT.md` — full breakdown of 18 files; CaS pipeline, pure-transition model, decorator pattern, background work choreography.
- `src/internal/ARCH_REVIEW.md` — two candidates: `service.rs` placement relative to `rpc.rs`; implicit `Blob` decorator construction order.

## What should bubble up to `src/CONTEXT.md`

- `mz-persist-client` is the primary persistence substrate; it sits below `mz-storage-controller` and `mz-txn-wal` in the dependency graph.
- The crate exposes a clean `Blob`/`Consensus` abstraction boundary inherited from `mz-persist`; all storage-backend switching happens at that seam.
- The `PersistClientCache` is a critical shared resource (connection pooling); callers must not create multiple caches per process.
- Schema evolution is first-class: `compare_and_evolve_schema` + `SchemaCache` provides backward-compatible migration with N²-migration caching.

## Cross-references

- `mz-persist` — `Blob`, `Consensus` traits.
- `mz-persist-types` — `Codec`, `Codec64`, columnar format.
- `mz-dyncfg` — runtime config.
- `mz-txn-wal`, `mz-storage-controller` — primary consumers.
- Generated developer docs: `doc/developer/generated/persist-client/`.
