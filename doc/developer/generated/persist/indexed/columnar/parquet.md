---
source: src/persist/src/indexed/columnar/parquet.rs
revision: fa066df3e5
---

# persist::indexed::columnar::parquet

Encodes and decodes `BlobTraceBatchPart` values to/from the Parquet format using Arrow as an intermediate layer.
Inline batch metadata is serialised as a base64-encoded proto stored in the Parquet file's key-value metadata under the `MZ:inline` key.
`EncodingConfig` controls which Parquet compression codec to use; the writer always sets Parquet v2 format and disables statistics to keep files compact.
