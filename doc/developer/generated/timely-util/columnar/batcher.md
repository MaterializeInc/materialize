---
source: src/timely-util/src/columnar/batcher.rs
revision: 32d39e677f
---

# timely-util::columnar::batcher

Defines two `ContainerBuilder` types and one `Merger` type for sorting and consolidating columnar update batches `(data, time, diff)`.

`Chunker<C>` is generic over a container type `C`; its primary `PushInto` implementation accepts `&mut Column<(D, T, R)>` when `C` is `ColumnationStack<(D, T, R)>`: it sorts by `(data, time)`, accumulates diffs for equal keys via `Semigroup::plus_equals`, and discards zero-diff entries, producing cleaned, deduplicated output batches for the differential dataflow merge batcher pipeline.

`ColumnChunker<U>` is the columnar-native counterpart: it consolidates `Column<(D, T, R)>` inputs and emits sorted, consolidated `ColumnBody<U>` chunks without round-tripping through columnation. This is where records leave the edge container: the input is whatever the edge delivered, the output is a body the batcher chains.

`MergeChunk<C>` is a trait abstracting the write side of a merge target, implemented by both `ColumnBody<C>` and `Column<C>`. It exposes `typed` (mutably access the typed container, materializing a serialized body first if needed), `view` (a borrowed columnar view), and `is_empty`.

`ColumnMerger<D, T, R>` implements `Merger` for `ColumnBody<(D, T, R)>` chunks. The free functions `merge_from` and `extract` operate over any `MergeChunk` impl, enabling code reuse between the `ColumnBody`-based merger and the `Column`-based column pager path. `merge_from` merges sorted consolidated inputs using gallop-accelerated bulk copies and a mid-merge amortized ship-threshold check; `extract` partitions records by an `upper` frontier into `keep` and `ship` containers. The `Merger::merge` implementation includes a whole-chunk passthrough fast path for disjoint-range chains. `empty_chunk` and `recycle_chunk` are `pub(crate)` for use in `merge_batcher`. The `Merger` accounting methods: `len` returns record count; `allocation` returns a `(size, capacity, allocations)` triple using serialized byte footprint for both size and capacity.
