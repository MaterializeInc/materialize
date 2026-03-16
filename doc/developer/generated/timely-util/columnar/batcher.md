---
source: src/timely-util/src/columnar/batcher.rs
revision: a67213e0ae
---

# timely-util::columnar::batcher

Defines `Chunker<C>`, a `ContainerBuilder` that sorts and consolidates columnar update batches `(data, time, diff)` before yielding them as ready containers.
`Chunker` accepts `&mut Column<(D, T, R)>` via `PushInto`, sorts by `(data, time)`, accumulates diffs for equal keys, and discards zero-diff entries, providing cleaned, deduplicated output batches for the differential dataflow merge batcher pipeline.
