---
source: src/timely-util/src/lib.rs
revision: 12181a5639
---

# timely-util

`mz-timely-util` provides Materialize-specific extensions and utilities for the timely dataflow and differential dataflow libraries.

Key modules:

* `builder_async` — async operator construction with `OperatorBuilder`, `AsyncInputHandle`, and shutdown coordination via `Button`.
* `hash` — `fixed_state()`, a fixed-seed `ahash::RandomState` used wherever deterministic hashing is required across runs, replicas, and builds (worker assignment during consolidation, per-column codec summaries in `mz_row_spine`).
* `operator` — extension traits `StreamExt` and `CollectionExt` for fallible maps, consolidation, and stream expiry; `ConcatenateFlatten` and `consolidate_pact`.
* `reclock` — timestamp translation operator for remapping source times to query times.
* `order` — `Partitioned<P, T>` timestamp, `Interval<P>`, `Reverse<T>`, and `refine_antichain`.
* `column_pager` — `ColumnPager` and policy types for spilling columnar chunks to a backing store under memory pressure; used by `Col2ValPagedBatcher`.
* `columnar` — `Column<C>` columnar container with submodules `batcher` (`Chunker`, `ColumnChunker`, `ColumnMerger`), `builder` (`ColumnBuilder`), `consolidate`, and `merge_batcher` (`ColumnMergeBatcher`) for batched aligned allocations; `Col2ValBatcher`, `Col2KeyBatcher`, and `Col2ValPagedBatcher` type aliases.
* `columnation` — columnation-based region storage including `ColumnationStack` and `ColInternalMerger`.
* `containers` — lgalloc-aware `alloc_aligned_zeroed` and `AccountedStackBuilder`.
* `probe` — frontier observation via `Handle<T>` with async and activator notifications.
* `replay` — `MzReplay` for replaying captured event streams with periodic re-activation.
* `activator` — `RcActivator` for threshold-gated external operator wakeup.
* `temporal` — `BucketChain` for efficient future-update storage by timestamp.
* `scope_label`, `pact`, `panic`, `antichain`, `capture` — profiling labels, round-robin distribution, graceful panic handling, pretty-printing, and Tokio capture adapters.

Key dependencies: `timely`, `differential-dataflow`, `columnar`, `columnation`, `mz-ore`, `tokio`, `custom-labels`.
Downstream consumers include `mz-storage`, `mz-compute`, and other crates in the Materialize dataflow stack.
