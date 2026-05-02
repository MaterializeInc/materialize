# persist-client

`mz-persist-client` — client library for Materialize's persist durability system.
Provides durable time-varying collections (shards) with snapshot-and-subscribe
semantics over parameterized `(K, V, T, D)` codec types.

Primary intended consumers: `mz-storage-controller`, `mz-txn-wal`, `mz-adapter`.
Code outside storage should not call persist directly; the storage layer mediates.

## Crate layout (LOC ≈ 44,363)

| Path | LOC | Role |
|---|---|---|
| `src/` | 43,390 | All Rust source — see [`src/CONTEXT.md`](src/CONTEXT.md) |
| `benches/` | ~879 | Criterion benchmarks (`benches.rs`, `porcelain.rs`, `plumbing.rs`) |
| `tests/` | ~0 | Integration test entry point |
| `build.rs` | — | Proto codegen for PubSub gRPC stubs |

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
- `src/CONTEXT.md` — complete per-file table for all top-level and submodule files; write/read paths; key concepts.

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
