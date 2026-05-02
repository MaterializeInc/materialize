---
source: src/timely-util/src/lib.rs
revision: f498b6e141
---

# timely-util

`mz-timely-util` provides Materialize-specific extensions and utilities for the timely dataflow and differential dataflow libraries.

Key modules:

* `builder_async` — async operator construction with `OperatorBuilder`, `AsyncInputHandle`, and shutdown coordination via `Button`.
* `operator` — extension traits `StreamExt` and `CollectionExt` for fallible maps, consolidation, and stream expiry; `ConcatenateFlatten` and `consolidate_pact`.
* `reclock` — timestamp translation operator for remapping source times to query times.
* `order` — `Partitioned<P, T>` timestamp, `Interval<P>`, `Reverse<T>`, and `refine_antichain`.
* `columnar` — `Column<C>` columnar container with submodules `batcher` (`Chunker`) and `builder` (`ColumnBuilder`) for batched aligned allocations; `Col2ValBatcher` and `Col2KeyBatcher` type aliases.
* `columnation` — columnation-based region storage including `ColumnationStack` and `ColInternalMerger`.
* `containers` — lgalloc-aware `alloc_aligned_zeroed` and `AccountedStackBuilder`.
* `probe` — frontier observation via `Handle<T>` with async and activator notifications.
* `replay` — `MzReplay` for replaying captured event streams with periodic re-activation.
* `activator` — `RcActivator` for threshold-gated external operator wakeup.
* `temporal` — `BucketChain` for efficient future-update storage by timestamp.
* `scope_label`, `pact`, `panic`, `antichain`, `capture` — profiling labels, round-robin distribution, graceful panic handling, pretty-printing, and Tokio capture adapters.

Key dependencies: `timely`, `differential-dataflow`, `columnar`, `columnation`, `mz-ore`, `tokio`, `custom-labels`.
Downstream consumers include `mz-storage`, `mz-compute`, and other crates in the Materialize dataflow stack.
