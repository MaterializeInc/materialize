# timely-util (mz-timely-util)

Materialize-specific extensions and helpers layered atop
[timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) and
differential-dataflow. Provides async operator construction, columnar
containers, reclocking, and miscellaneous dataflow utilities used pervasively
by `mz-storage` and `mz-compute`.

## Module surface (LOC ≈ 5,820)

| Module | LOC | Purpose |
|---|---|---|
| `reclock.rs` | 1,239 | Timestamp translation (`FromTime` → `IntoTime`) via remap collection; formal correctness invariants in doc comments |
| `builder_async.rs` | 895 | Async timely operator construction: `OperatorBuilder`, `AsyncInputHandle`, `Button` shutdown token |
| `operator.rs` | 774 | `StreamExt` / `CollectionExt` extension traits; `consolidate_pact`, `ConcatenateFlatten`, stream expiry |
| `columnation.rs` | 666 | `ColumnationStack`, `ColInternalMerger` — region-based storage for differential merging |
| `order.rs` | 437 | `Partitioned<P,T>`, `Interval<P>`, `Reverse<T>`, `refine_antichain` |
| `temporal.rs` | 348 | `BucketChain` — future-update storage bucketed by timestamp |
| `columnar.rs` | 299 | `Column<C>` container; `Col2ValBatcher` / `Col2KeyBatcher` type aliases |
| `probe.rs` | 207 | `Handle<T>` — async and activator-based frontier observation |
| `replay.rs` | 169 | `MzReplay` — replays captured event streams with periodic re-activation |
| `activator.rs` | 140 | `RcActivator` — threshold-gated external wakeup |
| `columnar/batcher.rs` | 116 | `Chunker` — batched aligned allocations for columnar containers |
| `columnar/builder.rs` | 112 | `ColumnBuilder` — builder for `Column<C>` |
| `pact.rs` | 85 | Round-robin and hash distribution pacts |
| `antichain.rs` | 71 | Pretty-printing and utilities for antichains |
| `containers.rs` | 52 | `alloc_aligned_zeroed`, `AccountedStackBuilder` (lgalloc-aware) |
| `containers/stack.rs` | 62 | Stack allocator used by containers |
| `capture.rs` | 41 | Tokio capture adapters |
| `panic.rs` | 41 | Graceful panic handling within timely workers |
| `scope_label.rs` | 33 | Profiling scope labels |

## Package identity

Crate name: `mz-timely-util`. No binary. Downstream consumers:
`mz-storage`, `mz-compute`, `mz-storage-operators`, `mz-clusterd`.

## Key interfaces (exported)

- **`builder_async`** — `OperatorBuilder`, `AsyncInputHandle`, `Button`
  (shutdown coordination); the primary Interface for async timely operators.
- **`operator::{StreamExt, CollectionExt}`** — extension traits for fallible
  maps, consolidation helpers, stream expiry, and `consolidate_pact`.
- **`reclock`** — timestamp-translation operator; maps `FromTime` source
  timestamps to `IntoTime` query timestamps via a remap collection.
- **`order::{Partitioned<P,T>, Interval<P>, Reverse<T>, refine_antichain}`** —
  composite and reversed timestamp types.
- **`columnar::{Column<C>, Col2ValBatcher, Col2KeyBatcher}`** — lgalloc-backed
  columnar containers with `Chunker` batcher and `ColumnBuilder`.
- **`columnation::{ColumnationStack, ColInternalMerger}`** — columnation-region
  storage for differential-dataflow merging.
- **`probe::Handle<T>`** — async and activator-based frontier observation.
- **`temporal::BucketChain`** — efficient future-update storage bucketed by
  timestamp.
- **`replay::MzReplay`** — replays captured event streams with periodic
  re-activation.
- **`activator::RcActivator`** — threshold-gated external operator wakeup.
- **`pact`** — round-robin and hash distribution policies.
- **`panic`** — graceful panic handling within timely workers.

## Dependencies

`timely`, `differential-dataflow`, `columnar`, `columnation`, `mz-ore`,
`tokio`, `custom-labels`, `lgalloc`.

## Bubbled findings for src/CONTEXT.md

- **`reclock.rs` is the heaviest module** (1,239 LOC) with detailed formal
  math in its doc comments; it is a significant Depth item — correctness
  depends on the `𝐑(t1) ⪯ 𝐑(t2)` monotonicity invariant being upheld by
  callers.
- **`builder_async` is the primary async-operator Seam**: any storage/compute
  operator that needs async I/O goes through `OperatorBuilder` here; changes
  propagate widely.
- **Columnar vs columnation split**: two parallel memory-management strategies
  (`columnar/` using lgalloc-aligned allocs vs `columnation/` using region
  storage) coexist with no single abstraction bridging them; a future
  Leverage/consolidation opportunity.
