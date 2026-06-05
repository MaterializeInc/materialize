---
source: src/compute/src/sink/correction_v2.rs
revision: 72d1410a40
---

# mz-compute::sink::correction_v2

An implementation of the correction buffer (`CorrectionV2`) used by the MV sink's `write_batches` operator with a chain-based design for amortized-efficient insertion, compaction, and iteration.

Updates are stored as sorted, consolidated `Chunk`s grouped into `Chain`s. Each `Chunk` is backed by a columnar region (a `Column<(D, Timestamp, Diff)>` produced by `ChunkBuilder`, which wraps `mz_timely_util::columnar::builder::ColumnBuilder`) so that variable-length payload lives in a single allocation per chunk and memory can be transparently spilled to disk. A "chain invariant" ensures each chain has at least `chain_proportionality` times as many updates as the next, producing a logarithmic hierarchy similar to an LSM tree. The invariant is maintained on update counts rather than chunk counts, because chunks are byte-bounded: any chain below the byte boundary is a single chunk regardless of update count. New updates are first accumulated in a `Stage` buffer; once enough updates fill a chunk they are sorted, consolidated, and inserted into the chain list, restoring the invariant via k-way merges.

The `Data` trait bounds require `D: Columnar` with a container that supports `Send + Sync + Clone` and `Ref`-level `Eq + Ord`, allowing merge and heap code to compare updates through columnar borrows without cloning. `CorrectionV2` is generic over `D: Data`.

Retrieving consolidated updates before a given `upper` merges only the relevant prefixes of each chain, leaving future-timestamped data untouched. Compaction respects the `since` frontier by splitting chains at each distinct time at or before `since` and merging per-time sub-chains separately, to preserve `(time, data)` sort order after time advancement. Insert has amortized O(log N) complexity; retrieval is O(U log K) where U is the number of updates before `upper` and K is the number of chains.
