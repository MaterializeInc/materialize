---
source: src/timely-util/src/columnation.rs
revision: f498b6e141
---

# timely-util::columnation

Columnation-based containers vendored from `differential-dataflow`, providing columnar storage for timely/differential operator pipelines.

**`ColumnationStack<T>`** — an append-only vector that stores `T: Columnation` elements using shared region allocations. Elements are kept in a `Vec<T>` whose pointers reference a shared inner `T::InnerRegion`, avoiding per-element heap allocation. Implements `BatchContainer`, `BuilderInput`, `DrainContainer`, `SizableContainer`, `Accountable`, and `InternalMerge`, making it usable as a differential-dataflow trace container. Provides `copy`, `copy_destructured`, `retain_from`, `heap_size`, `summed_heap_size`, `reserve_items`, and `reserve_regions`.

**`ColumnationChunker<(D, T, R)>`** — a `ContainerBuilder` that accumulates updates into a flat `Vec` for efficient in-place sorting and consolidation via `consolidate_updates`, then copies consolidated results into fixed-size `ColumnationStack` chunks (target size 64 KiB). Implements `PushInto<&mut Vec<(D, T, R)>>`.

**`ColInternalMerger<D, T, R>`** — a type alias for `differential_dataflow::trace::implementations::merge_batcher::InternalMerger<ColumnationStack<(D, T, R)>>`, used as the merge strategy in `Col2ValBatcher` and `Col2KeyBatcher`.
