# persist-client::internal

Private implementation layer of `mz-persist-client`. Owns the shard state machine,
consensus/blob interaction, background workers, encoding, and metrics.
No subdirectories; all 23,526 LOC lives directly in this directory.

## Files (LOC ≈ 23,526)

| File | What it owns |
|---|---|
| `state.rs` (4,418) | Core data model: `State`/`TypedState`, `StateCollections`, `HollowBatch`/`HollowBatchPart`/`HollowRun`. All state transitions are pure functions returning a new `State`. |
| `metrics.rs` (3,252) | `Metrics` and ~20 sub-structs for every operation; `MetricsBlob` / `MetricsConsensus` decorator impls of `Blob`/`Consensus` traits; `ShardsMetrics` per-shard gauge vectors. |
| `trace.rs` (2,646) | `Trace` — persist's fork of Differential Dataflow's `Spine`. Asynchronous compaction via `FueledMergeReq`/`FueledMergeRes`; N-way merge via `SpineBatch`. |
| `machine.rs` (2,641) | `Machine` — retry loop driving all state transitions; calls pure functions, handles CaS failures, schedules maintenance. Also owns `retry_external`/`retry_determinate` backoff helpers. |
| `encoding.rs` (2,343) | Proto serialization for `State`, `StateDiff`, `Trace`, rollups, reader/writer state. `LazyProto`/`LazyPartStats` for deferred decoding. Codec version compatibility guards. |
| `state_diff.rs` (1,709) | `StateDiff` — field-level insert/update/delete diff between consecutive `State` versions; primary unit over PubSub. |
| `compact.rs` (1,347) | `Compactor` — background worker consuming `CompactReq`, reading/consolidating/writing parts on `IsolatedRuntime`, respecting memory budget. |
| `state_versions.rs` (1,333) | `StateVersions` — durable log: consensus holds diffs, blob holds rollups. Init, append, fetch-by-replay, truncate. Invariant: every live diff range has a covering rollup. |
| `apply.rs` (729) | `Applier` — the narrow CaS seam: executes compare-and-set against `StateVersions`, updates `StateCache`, publishes diffs to PubSub. All state mutations flow through here. |
| `gc.rs` (720) | `GarbageCollector` — deletes unreachable blob objects and truncates consensus on a `GcReq`, with configurable delete concurrency. |
| `cache.rs` (558) | `BlobMemCache` — LRU in-memory cache wrapping `Blob`, dynamically sized by worker count. Implements `Blob`. |
| `watch.rs` (469) | `StateWatchNotifier` / `StateWatch` — reactive SeqNo notifications via Tokio broadcast; falls back to consensus polling when lagging. |
| `paths.rs` (418) | Key-naming scheme for all blob objects and consensus entries: `PartId`, `RollupId`, `WriterKey`, `BlobKey`. |
| `datadriven.rs` (300) | Test harness for data-driven tests of internal operations. |
| `merge.rs` (263) | `MergeTree<T>` — bounded-depth binary merge tree used during batch building to cap outstanding parts. |
| `maintenance.rs` (245) | `RoutineMaintenance` / `WriterMaintenance` — accumulate GC, rollup, compaction work to execute after a successful CaS. |
| `restore.rs` (120) | `restore_blob` — disaster-recovery helper that re-uploads blobs referenced by consensus but missing from blob storage. |
| `service.rs` (15) | Re-exports protobuf-generated gRPC stubs for the PubSub service; implementation lives in `rpc.rs`. |

## Key concepts

- **CaS pipeline.** Every state mutation follows: `Machine` (retry loop) → `Applier` (CaS against `StateVersions`) → `StateVersions` (consensus compare-and-set) → `StateCache` update + PubSub publish.
- **Pure state transitions.** `StateCollections` mutation functions are pure (take/return state); `Machine` retries when consensus detects a race. No in-place mutation under lock except via `LockingTypedState`.
- **Decorator pattern.** `MetricsBlob` and `MetricsConsensus` wrap the `Blob`/`Consensus` trait objects to add instrumentation without touching callsites. `BlobMemCache` similarly wraps `Blob` to add a caching layer.
- **Background work choreography.** After each successful CaS, `Applier` returns a `RoutineMaintenance` to `Machine`, which fans out to `GarbageCollector`, rollup writer, and `Compactor` without blocking the write path.
- **`Trace` / Spine.** Compaction is decoupled: `Trace` emits `FueledMergeReq` events; `Compactor` processes them asynchronously; results arrive via `apply_merge_res`.

## Cross-references

- Public handles (`WriteHandle`, `ReadHandle`, `SinceHandle`) in `src/` each own a `Machine` and invoke it for all operations.
- `state_versions.rs` depends on `mz-persist` (`Blob`, `Consensus` traits).
- `service.rs` / `rpc.rs` divide the PubSub gRPC surface: generated stubs here, implementation in parent.
- Generated developer docs: `doc/developer/generated/persist-client/internal/`.
