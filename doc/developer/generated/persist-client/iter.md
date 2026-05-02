---
source: src/persist-client/src/iter.rs
revision: 2a6ac3ab4c
---

# persist-client::iter

Provides `Consolidator`, a streaming consolidation iterator over multiple sorted batch parts, and `StructuredSort`, a trait abstracting row ordering for columnar data.
Parts are fetched concurrently via `FuturesUnordered`, merged using a `BinaryHeap`, and consolidated in sorted order to produce deduplicated `(key, val, time, diff)` tuples.
The minimum consolidated version constant (`MINIMUM_CONSOLIDATED_VERSION`) guards against replaying parts written by older buggy consolidation implementations.
