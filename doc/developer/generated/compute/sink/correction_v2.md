---
source: src/compute/src/sink/correction_v2.rs
revision: c96d4ef054
---

# mz-compute::sink::correction_v2

An implementation of the correction buffer (`CorrectionV2`) used by the MV sink's `write_batches` operator with a chain-based design for amortized-efficient insertion, compaction, and iteration.

Updates are stored as sorted, consolidated `Chunk`s grouped into `Chain`s. Each `Chunk` is backed by a columnar region (a `Column<(D, Timestamp, Diff)>`) so that variable-length payload lives in a single allocation per chunk and memory can be transparently spilled to disk. A "chain invariant" ensures each chain in a bucket has at least `chain_proportionality` times as many updates as the next, producing a logarithmic hierarchy similar to an LSM tree. New updates are first accumulated in a `Stage` buffer; once enough updates fill a chunk they are sorted, consolidated, and routed.

`CorrectionV2` holds updates in three places:
- A `BucketChain` partitions times at or beyond the `boundary` (the largest read `upper` seen so far) into buckets of exponentially growing time ranges. Reads only touch buckets below their `upper`, so far-future updates such as temporal-filter retractions are rarely accessed.
- `pending_low` holds chains at times below the `boundary` that have not yet been emitted (mostly persist feedback insertions).
- `emitted` is a single chain holding the updates returned by the last read, kept separate until their feedback retractions arrive so that future-timestamped data is never re-merged during a read.

The `Data` trait bounds require `D: Columnar` with a container that supports `Send + Sync + Clone` and `Ref`-level `Eq + Ord`, allowing merge and heap code to compare updates through columnar borrows without cloning. `CorrectionV2` is generic over `D: Data`.

Retrieving consolidated updates before a given `upper` peels buckets off the bucket chain, splits those chains and `pending_low` at the `upper`, merges the parts below the `upper` into the new `emitted` chain, and returns an iterator over it; updates at times at or beyond `upper` are never touched. Compaction respects the `since` frontier by splitting chains at each distinct stale time and merging per-time sub-chains separately to preserve `(time, data)` sort order; for a large number of stale times the affected updates are materialized and sorted in one O(U log U) pass instead. Insert has amortized O(log N) complexity; retrieval is O(U log K) where U is the number of updates before `upper` and K is the number of chains containing them.
