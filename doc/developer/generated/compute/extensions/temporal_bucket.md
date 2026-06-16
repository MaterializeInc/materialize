---
source: src/compute/src/extensions/temporal_bucket.rs
revision: a1b9ee861f
---

# mz-compute::extensions::temporal_bucket

Implements `TemporalBucketing`, a stream extension that delays updates into a `BucketChain` and reveals them only after the input frontier advances past a configurable threshold relative to the `as_of`.
This enables temporal filters (e.g., `mz_now()` predicates) to buffer future-timestamped updates without holding capabilities at every individual future time.
`MergeBatcherWrapper` wraps a `ColumnMergeBatcher` (the same columnar-native batcher used by default arrangements) to implement the `Bucket` trait, supporting time-based splitting and sealed output. Input `Vec` updates are staged into a `Column` before being fed through a `ColumnChunker`, and sealed chunks are `Column` values whose records are reconstituted into owned tuples for the output session.
