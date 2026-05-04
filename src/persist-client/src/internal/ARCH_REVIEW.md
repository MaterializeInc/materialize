# Architecture review — `persist-client::internal`

Scope: `src/persist-client/src/internal/` (≈ 23,526 LOC of Rust).

## 1. `service.rs` is a 15-line shim whose implementation lives in the parent

**Files**
- `src/persist-client/src/internal/service.rs:12-15` — single `include!` macro, re-exports generated gRPC stubs.
- `src/persist-client/src/rpc.rs:49-51` — sole consumer, immediately `use`s `crate::internal::service::...`.

**Problem**
The generated stubs have no logical relationship to `internal/`; they are PubSub gRPC types and exist here only because the protobuf namespace is `mz_persist_client.internal.service`. The implementation (`PubSubSender`, `PubSubReceiver`, `GrpcPubSubClient`, `SubscriptionTrackingSender`) lives in the parent-level `rpc.rs`. The file creates an asymmetric Module split — callers navigate `internal::service` to reach types that are conceptually owned by `rpc.rs`.

**Deletion test passes.** Moving `service.rs` up to the crate root and adjusting `use` paths is relocation, not deepening, so this is borderline. The interesting observation is that the seam would improve Locality: `rpc.rs` and its generated stubs would live at the same level.

**Risk.** Proto codegen output path is `OUT_DIR/mz_persist_client.internal.service.rs`; the namespace encoding comes from the `.proto` file. Moving the file would require either renaming the proto package or leaving the `internal` namespace discrepancy. Not a free relocation.

## 2. `BlobMemCache` duplicates the `Blob` decorator pattern already used by `MetricsBlob`

**Files**
- `src/persist-client/src/internal/cache.rs:26,103` — `pub struct BlobMemCache` implements `Blob` by wrapping `Arc<dyn Blob>`.
- `src/persist-client/src/internal/metrics.rs:2771,2787` — `pub struct MetricsBlob` implements `Blob` by wrapping `Arc<dyn Blob>`.

**Problem**
Both types are `Blob`-wrapping decorators with the same shape: `struct Foo { blob: Arc<dyn Blob>, ... }; impl Blob for Foo { ... }`. `BlobMemCache` lives in `internal/` while `MetricsBlob` lives in `internal/metrics.rs`; neither is composable as a Leverage point — the cache is wired in `cache.rs` (parent level) before the metrics decorator. There is no shared `BlobLayer` or `BlobMiddleware` interface that makes the chain explicit or extensible. This is a minor seam gap rather than a deep architectural flaw — the chain is correct, just implicit.

**Solution sketch**
Document the expected decoration order (`MetricsBlob(BlobMemCache(raw_blob))`) as an explicit constructor in `PersistClientCache`. No structural change needed to benefit from better Locality; a comment or `type` alias at the construction site would suffice.

## 3. (Honest skip) `Machine` / `Applier` split

`Machine` holds an `Applier` and drives the retry loop; `Applier` holds `LockingTypedState` and executes CaS. The split gives a clean Seam: `Machine` = retry + scheduling policy; `Applier` = shared-state access. The `apply_unbatched_cmd` closure argument (`WorkFn: FnMut(SeqNo, &PersistConfig, &mut StateCollections<T>)`) is uniform across ~15 call sites. No deepening opportunity here — the two-layer split is already doing its job.

## 4. (Honest skip) `maintenance.rs` — `#[must_use]` struct

`RoutineMaintenance` carries `#[must_use]` and is returned from every `Machine` call. Callers explicitly call `start_performing` or `perform`. This is a disciplined pattern, not a friction point.

## What this review did not reach

- `trace.rs` internals (Spine level-balance heuristics, `N`-way merge correctness) — would require domain-level review of compaction scheduling.
- `encoding.rs` proto compatibility matrix — worth a dedicated review before any state format change.
