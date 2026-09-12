---
source: src/compute/src/extensions/temporal_bucket.rs
revision: 2564b843df
---

# mz-compute::extensions::temporal_bucket

Implements `TemporalBucketing`, a stream extension that delays updates into a `BucketChain` and reveals them only after the input frontier advances past a configurable threshold relative to the `as_of`.
This enables temporal filters (e.g., `mz_now()` predicates) to buffer future-timestamped updates without holding capabilities at every individual future time.
`MergeBatcherWrapper` wraps a `ColumnMergeBatcher` (the same columnar-native batcher used by default arrangements) to implement the `Bucket` trait, supporting time-based splitting and sealed output. The primary implementation operates on `Stream<Column<(D, T, Diff)>>` directly; a secondary implementation for `StreamVec<(D, T, Diff)>` is provided for callers whose consumers still expect owned records, staging only the retained (non-pass-through) updates through the columnar chain batcher. Sealed chunks are `Column` values given to the output session.
