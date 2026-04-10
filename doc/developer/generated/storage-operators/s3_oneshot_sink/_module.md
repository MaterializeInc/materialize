---
source: src/storage-operators/src/s3_oneshot_sink.rs
revision: f498b6e141
---

# storage-operators::s3_oneshot_sink

Renders the three-operator dataflow (initialization → upload → completion) that writes a consolidated collection to S3 as part of `COPY TO`.
The initialization operator runs on a leader worker, checks for upstream errors and writes a sentinel file; the upload operator writes row batches partitioned by hash; the completion operator removes the sentinel file and invokes a per-worker callback with the final row count.
The `CopyToS3Uploader` trait abstracts over format-specific upload logic; `parquet` and `pgcopy` submodules provide the two concrete implementations.
`CopyToParameters` carries tuneable buffer and part-size knobs forwarded from compute dynamic configs.
