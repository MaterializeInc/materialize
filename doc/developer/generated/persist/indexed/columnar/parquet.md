---
source: src/persist/src/indexed/columnar/parquet.rs
revision: efa37c1719
---

# persist::indexed::columnar::parquet

Encodes and decodes `BlobTraceBatchPart` values to/from the Parquet format using Arrow as an intermediate layer.
Inline batch metadata is serialised as a base64-encoded proto stored in the Parquet file's key-value metadata under the `MZ:inline` key.
`EncodingConfig` controls which Parquet compression codec to use; the writer sets Parquet v2 format, a 1 MiB data page size limit, no row-group row-count cap (`set_max_row_group_row_count(None)`), and disables statistics to keep files compact.
