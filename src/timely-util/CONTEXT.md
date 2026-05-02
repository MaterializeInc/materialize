# timely-util (mz-timely-util)

Materialize-specific extensions and helpers layered atop
[timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) and
differential-dataflow. Provides async operator construction, columnar
containers, reclocking, and miscellaneous dataflow utilities used pervasively
by `mz-storage` and `mz-compute`.

## Subtree (≈ 5,947 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 5,820 | All modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

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
