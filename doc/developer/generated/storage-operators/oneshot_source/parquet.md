---
source: src/storage-operators/src/oneshot_source/parquet.rs
revision: 1f5cd0d026
---

# storage-operators::oneshot_source::parquet

Implements `OneshotFormat` for Parquet as `ParquetFormat`, with `ParquetWorkRequest` and `ParquetRowGroup` as its associated work and chunk types.
`split_work` fetches Parquet file metadata via a `ParquetReaderAdapter` (an `AsyncFileReader`/`MetadataFetch` bridge over any `OneshotSource`) and emits one work request per row group, enabling parallel fetching; `decode_chunk` converts each `RecordBatch` into `Row`s using `mz_arrow_util::reader::ArrowReader`.
