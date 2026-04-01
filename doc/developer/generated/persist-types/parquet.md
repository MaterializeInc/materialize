---
source: src/persist-types/src/parquet.rs
revision: 5f785f23fd
---

# persist-types::parquet

Provides `encode_arrays` and `decode_arrays` for Parquet serialization and deserialization of Arrow arrays, plus `EncodingConfig`, `CompressionFormat`, and `CompressionLevel` types for configuring the Parquet writer.
`CompressionFormat::from_str` parses dynamic-config strings such as `"zstd-3"` or `"snappy"` into the appropriate compression variant.
