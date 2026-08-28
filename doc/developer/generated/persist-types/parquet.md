---
source: src/persist-types/src/parquet.rs
revision: efa37c1719
---

# persist-types::parquet

Provides `encode_arrays` and `decode_arrays` for Parquet serialization and deserialization of Arrow arrays, plus `EncodingConfig`, `CompressionFormat`, and `CompressionLevel` types for configuring the Parquet writer.
The writer uses Parquet v2 format, a 1 MiB data page size limit, and no row-group row-count cap (`set_max_row_group_row_count(None)`), placing all rows in a single row group per file.
`CompressionFormat::from_str` parses dynamic-config strings into the appropriate compression variant; plain names without a level suffix (e.g. `"snappy"`, `"zstd"`, `"brotli"`, `"gzip"`) select the default level, while leveled strings (e.g. `"zstd-3"`) select a specific level.
