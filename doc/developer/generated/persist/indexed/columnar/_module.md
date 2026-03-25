---
source: src/persist/src/indexed/columnar.rs
revision: e757b4d11b
---

# persist::indexed::columnar

Defines `ColumnarRecords` and `ColumnarRecordsBuilder`, the canonical in-memory columnar representation of `((Key, Val), Time, Diff)` persist updates stored as Arrow binary and `i64` arrays.
Key and value data are bounded by `KEY_VAL_DATA_MAX_LEN` (i32::MAX bytes) due to Parquet page-size constraints.
The `arrow` sub-module handles Arrow `RecordBatch` encoding, and the `parquet` sub-module handles Parquet file encoding/decoding.
`ColumnarRecordsStructuredExt` carries optional structured (schema-aware) key and value columns alongside the codec-encoded columns.
