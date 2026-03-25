---
source: src/compute/src/sink/copy_to_s3_oneshot.rs
revision: e79a6d96d9
---

# mz-compute::sink::copy_to_s3_oneshot

Implements the `CopyToS3Oneshot` sink, which writes a snapshot of a collection to S3 (or compatible object storage) in Parquet format as a one-shot operation initiated by a `COPY TO` statement.
Updates are consolidated per worker and exchanged to a single writer worker that accumulates row groups, respecting configured part size and row-group ratio dyncfgs.
A `CopyToResponse` is returned to the controller with the number of rows written once the upload completes.
