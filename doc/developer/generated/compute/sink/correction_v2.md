---
source: src/compute/src/sink/correction_v2.rs
revision: 4d8deb2de7
---

# mz-compute::sink::correction_v2

An alternative implementation of the correction buffer (`CorrectionV2`) with a chain-based design for amortized-efficient insertion, compaction, and iteration.

Updates are stored as sorted, consolidated `Chunk`s grouped into `Chain`s. A "chain invariant" ensures each chain has at least `CHAIN_PROPORTIONALITY` times as many chunks as the next, producing a logarithmic hierarchy similar to an LSM tree. New updates are first accumulated in a `Stage` buffer; once a full chunk is ready it is sorted and merged into the chain list, restoring the invariant via k-way merges. The byte capacity of each `Chunk` is controlled by the `CORRECTION_V2_CHUNK_SIZE` dyncfg and is fixed at construction time.

Retrieving consolidated updates before a given `upper` merges only the relevant prefixes of each chain, leaving future-timestamped data untouched. Compaction respects the `since` frontier by merging per-time sub-chains separately to preserve sort order after time advancement. Insert has amortized O(log N) complexity; retrieval is O(U log K) where U is the number of updates before `upper` and K is the number of chains.
