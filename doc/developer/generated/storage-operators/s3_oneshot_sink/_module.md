---
source: src/storage-operators/src/s3_oneshot_sink.rs
revision: e79a6d96d9
---

# storage-operators::s3_oneshot_sink

Renders the three-operator dataflow (initialization → upload → completion) that writes a consolidated collection to S3 as part of `COPY TO`.
The `CopyToS3Uploader` trait abstracts over format-specific upload logic; `parquet` and `pgcopy` submodules provide the two concrete implementations.
`CopyToParameters` carries tuneable buffer and part-size knobs forwarded from compute dynamic configs.
