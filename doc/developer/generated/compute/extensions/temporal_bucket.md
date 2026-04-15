---
source: src/compute/src/extensions/temporal_bucket.rs
revision: b0fa98e931
---

# mz-compute::extensions::temporal_bucket

Implements `TemporalBucketing`, a stream extension that delays updates into a `BucketChain` and reveals them only after the input frontier advances past a configurable threshold relative to the `as_of`.
This enables temporal filters (e.g., `mz_now()` predicates) to buffer future-timestamped updates without holding capabilities at every individual future time.
`MergeBatcherWrapper` wraps a differential `MergeBatcher` to implement the `Bucket` trait, supporting time-based splitting and sealed output.
