---
source: src/persist/src/indexed/columnar/arrow.rs
revision: ddc1ff8d2d
---

# persist::indexed::columnar::arrow

Encodes and decodes `BlobTraceUpdates` as Arrow `RecordBatch`es.
`encode_arrow_batch` maps codec-encoded key/value binary columns plus structured key/value extension columns into a record batch, placing timestamps and diffs as `i64` columns for better compression.
`decode_arrow_batch` reverses the process, optionally reallocating arrays into lgalloc regions controlled by two `dyncfg` flags (`ENABLE_ARROW_LGALLOC_CC_SIZES`, `ENABLE_ARROW_LGALLOC_NONCC_SIZES`).
