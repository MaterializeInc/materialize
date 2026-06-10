---
source: src/persist-client/src/internal/merge.rs
revision: 2a6ac3ab4c
---

# persist-client::internal::merge

Provides `MergeTree<T>`, a bounded-depth binary merge tree used during batch building to limit the number of outstanding parts by merging adjacent parts when any level exceeds the configured maximum length.
The tree guarantees insertion-order preservation and `O(log N)` merge depth, with at most `K` parts returned by `finish`.
