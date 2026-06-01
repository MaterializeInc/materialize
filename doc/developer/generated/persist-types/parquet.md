---
source: src/persist-types/src/parquet.rs
revision: 4679a9aaf0
---

# persist-types::parquet

Provides `encode_arrays` and `decode_arrays` for Parquet serialization and deserialization of Arrow arrays, plus `EncodingConfig`, `CompressionFormat`, and `CompressionLevel` types for configuring the Parquet writer.
`CompressionFormat::from_str` parses dynamic-config strings into the appropriate compression variant; plain names without a level suffix (e.g. `"snappy"`, `"zstd"`, `"brotli"`, `"gzip"`) select the default level, while leveled strings (e.g. `"zstd-3"`) select a specific level.
