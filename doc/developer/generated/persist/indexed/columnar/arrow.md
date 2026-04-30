---
source: src/persist/src/indexed/columnar/arrow.rs
revision: b89a9e0ec5
---

# persist::indexed::columnar::arrow

Encodes and decodes `BlobTraceUpdates` as Arrow `RecordBatch`es.
`encode_arrow_batch` maps codec-encoded key/value binary columns plus structured key/value extension columns into a record batch, placing timestamps and diffs as `i64` columns for better compression.
`decode_arrow_batch` reverses the process, rebuilding each array via `realloc_array` / `realloc_any` (which drop null buffers from non-nullable fields as a workaround for an arrow-rs parquet decoding quirk).
The module also exposes `realloc_array` and `realloc_any` as standalone helpers for callers that need the same null-buffer cleanup on individual arrays.
