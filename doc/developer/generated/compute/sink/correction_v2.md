---
source: src/compute/src/sink/correction_v2.rs
revision: beb7f6c04b
---

# mz-compute::sink::correction_v2

An implementation of the correction buffer (`CorrectionV2`) used by the MV sink's `write_batches` operator with a chain-based design for amortized-efficient insertion, compaction, and iteration.

Updates are stored as sorted, consolidated `Chunk`s grouped into `Chain`s. Each `Chunk` holds a `ColumnBody` that is offered to the process buffer pool (`mz_ore::pool`) on construction; the pool may spill it under memory pressure, controlled by the `ENABLE_CORRECTION_V2_SPILL` dyncfg. Chunks are spilled at a depth that reflects how many chain merges they have survived: freshly staged chunks start at depth 0, and a bucket merge writes its output one generation deeper than the deepest input, so deeply merged chunks land in cooler eviction bands and are eligible for compression. A "chain invariant" ensures each chain in a bucket has at least `chain_proportionality` times as many updates as the next, producing a logarithmic hierarchy similar to an LSM tree. New updates are first accumulated in a `Stage` buffer that grows on demand; once the staged bytes (heap included) reach the configured `chunk_size`, they are sorted, consolidated, and routed.

`CorrectionV2` holds updates in three places:
- A `BucketChain` partitions times at or beyond the `boundary` (the largest read `upper` seen so far) into buckets of exponentially growing time ranges. Reads only touch buckets below their `upper`, so far-future updates such as temporal-filter retractions are rarely accessed.
- `pending_low` holds chains at times below the `boundary` that have not yet been emitted (mostly persist feedback insertions).
- `emitted` is a single chain holding the updates returned by the last read, kept separate until their feedback retractions arrive so that future-timestamped data is never re-merged during a read.

The `Data` trait bounds require `D: Columnar` with a container satisfying `DataContainer` (`Send + Sync + Clone` and `Ref`-level `Eq + Ord` plus `Borrowed`-level `Send`), and `DataBytes` (to size the staging area in bytes). The `Ref`-level `Eq + Ord` bounds let merge and heap code compare updates through columnar borrows without cloning; the `Borrowed`-level `Send` bound lets a hoisted `Chunk::view` travel with the iterators that `CorrectionV2::updates_before` hands across the persist writer's `await`. `CorrectionV2` is generic over `D: Data`.

Size accounting is O(1): an `Accounting` struct holds running totals (`records`, `size`, `allocations`) as atomics shared with every chain bucket and the stage, so `update_metrics` reads the totals directly without walking chains or paging chunks in. `Chain` tracks its own `size` field (maintained as chunks are pushed), so the accounting can add or subtract it without touching chunk bodies.

Merges (`merge_2` and `merge_many`) hoist `Chunk::view` once per chunk pair rather than once per update: `view` re-decodes the column header on every call, so the inner loop reads through the already-borrowed view and breaks to re-borrow only when a cursor crosses into its next chunk.

`Chain::iter` (used by `updates_before`) reads each chunk with `with_view`, which copies the body out of the pool for the call and drops the copy after, so the pool keeps its slot and may spill the body again; the chunk does not stay resident between reads.

Retrieving consolidated updates before a given `upper` peels buckets off the bucket chain, splits those chains and `pending_low` at the `upper`, merges the parts below the `upper` into the new `emitted` chain, and returns an iterator over it; updates at times at or beyond `upper` are never touched. Compaction respects the `since` frontier by splitting chains at each distinct stale time and merging per-time sub-chains separately to preserve `(time, data)` sort order; for a large number of stale times the affected updates are materialized and sorted in one O(U log U) pass instead. Insert has amortized O(log N) complexity; retrieval is O(U log K) where U is the number of updates before `upper` and K is the number of chains containing them.
