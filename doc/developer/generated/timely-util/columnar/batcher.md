---
source: src/timely-util/src/columnar/batcher.rs
revision: bfa6499c3b
---

# timely-util::columnar::batcher

Defines two `ContainerBuilder` types and one `Merger` type for sorting and consolidating columnar update batches `(data, time, diff)`.

`Chunker<C>` is generic over a container type `C`; its primary `PushInto` implementation accepts `&mut Column<(D, T, R)>` when `C` is `ColumnationStack<(D, T, R)>`: it sorts by `(data, time)`, accumulates diffs for equal keys via `Semigroup::plus_equals`, and discards zero-diff entries, producing cleaned, deduplicated output batches for the differential dataflow merge batcher pipeline.

`ColumnChunker<U>` is the columnar-native counterpart: it consolidates `Column<(D, T, R)>` inputs while keeping the output in `Column` format without round-tripping through columnation.

`ColumnMerger<D, T, R>` implements `Merger` for `Column<(D, T, R)>` chunks. It provides a `merge_from` method (on `Column<(D, T, R)>` inherently) that merges sorted, consolidated input columns into `self` using gallop-accelerated bulk copies and a mid-merge amortized ship-threshold check, and an `extract` method that partitions entries by an `upper` frontier into `keep` (times at or after `upper`) and `ship` (times strictly before `upper`) containers. The `Merger::merge` implementation also includes a whole-chunk passthrough fast path for disjoint-range chains. The trait bounds on `D` no longer require `Default` or the various `Push` bounds that were previously listed; the simplified bound set is `D: Columnar` with `Ref<'a, D>: Copy + Ord`. The helper functions `empty_chunk` and `recycle_chunk` are `pub(crate)` to allow use from the `merge_batcher` submodule.
