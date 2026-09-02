---
source: src/timely-util/src/columnation.rs
revision: 1c55de49eb
---

# timely-util::columnation

Columnation-based containers vendored from `differential-dataflow`, providing columnar storage for timely/differential operator pipelines.

**`ColumnationStack<T>`** — an append-only vector that stores `T: Columnation` elements using shared region allocations. Elements are kept in a `Vec<T>` whose pointers reference a shared inner `T::InnerRegion`, avoiding per-element heap allocation. Implements `BatchContainer`, `BuilderInput`, `DrainContainer`, `SizableContainer`, and `Accountable`, making it usable as a differential-dataflow trace container. Provides `copy`, `copy_destructured`, `retain_from`, `heap_size`, `summed_heap_size`, `reserve_items`, and `reserve_regions`.

**`ColumnationChunker<(D, T, R)>`** — a `ContainerBuilder` that accumulates updates into a flat `Vec` for efficient in-place sorting and consolidation via `consolidate_updates`, then copies consolidated results into fixed-size `ColumnationStack` chunks (target size 64 KiB). Implements `PushInto<&mut Vec<(D, T, R)>>`. The `ContainerBuilder` implementation provides two accounting methods: `len` returns the number of elements in a chunk (`chunk[..].len()`), and `allocation` returns a `(size, capacity, allocations)` triple computed by walking heap sizes via `ColumnationStack::heap_size`.

**`ColInternalMerger<D, T, R>`** — a struct implementing the `differential_dataflow::trace::implementations::merge_batcher::Merger` trait for `ColumnationStack<(D, T, R)>` chunks. It merges chains of sorted, consolidated `ColumnationStack` chunks by internal iteration (copying through `copy`/`copy_destructured` rather than draining owned tuples), and is used as the merge strategy in `Col2ValBatcher` and `Col2KeyBatcher`.
