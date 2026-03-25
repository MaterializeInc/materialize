---
source: src/storage-operators/src/s3_oneshot_sink/parquet.rs
revision: e757b4d11b
---

# storage-operators::s3_oneshot_sink::parquet

Implements `CopyToS3Uploader` for Parquet as `ParquetUploader`, which buffers incoming `Row`s into an `ArrowBuilder`, flushes to an `ArrowWriter` once the configurable `arrow_builder_buffer_bytes` threshold is reached, and uploads completed row groups via `S3MultiPartUploader`.
File rotation occurs when the estimated file size exceeds `max_file_size`; all buffer ratios and part sizes are controlled by `CopyToParameters`.
