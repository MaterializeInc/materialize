---
source: src/timely-util/src/columnar/batcher.rs
revision: f498b6e141
---

# timely-util::columnar::batcher

Defines `Chunker<C>`, a `ContainerBuilder` generic over a container type `C` that sorts and consolidates columnar update batches `(data, time, diff)` before yielding them as ready containers.
The primary `PushInto` implementation accepts `&mut Column<(D, T, R)>` when `C` is `ColumnationStack<(D, T, R)>`: it sorts by `(data, time)`, accumulates diffs for equal keys via `Semigroup::plus_equals`, and discards zero-diff entries, producing cleaned, deduplicated output batches for the differential dataflow merge batcher pipeline.
