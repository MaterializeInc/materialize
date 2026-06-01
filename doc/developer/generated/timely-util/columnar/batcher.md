---
source: src/timely-util/src/columnar/batcher.rs
revision: e45e744c63
---

# timely-util::columnar::batcher

Defines two `ContainerBuilder` types for sorting and consolidating columnar update batches `(data, time, diff)`.

`Chunker<C>` is generic over a container type `C`; its primary `PushInto` implementation accepts `&mut Column<(D, T, R)>` when `C` is `ColumnationStack<(D, T, R)>`: it sorts by `(data, time)`, accumulates diffs for equal keys via `Semigroup::plus_equals`, and discards zero-diff entries, producing cleaned, deduplicated output batches for the differential dataflow merge batcher pipeline.

`ColumnChunker<U>` is the columnar-native counterpart: it consolidates `Column<(D, T, R)>` inputs while keeping the output in `Column` format without round-tripping through columnation. It also implements `Merger` for `Column<(D, T, R)>` via a `merge_from` method that merges sorted, consolidated input columns into `self` and an `extract` method that partitions entries by an `upper` frontier into `keep` (times at or after `upper`) and `ship` (times strictly before `upper`) containers.
