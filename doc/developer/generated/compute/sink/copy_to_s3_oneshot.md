---
source: src/compute/src/sink/copy_to_s3_oneshot.rs
revision: b0fa98e931
---

# mz-compute::sink::copy_to_s3_oneshot

Implements the `CopyToS3Oneshot` sink, which writes a snapshot of a collection to S3 (or compatible object storage) in Parquet format as a one-shot operation initiated by a `COPY TO` statement.
Updates are consolidated per worker and exchanged by batch ID across workers, with each worker responsible for 0 or more batches; each batch is split into one or more files according to the configured `MAX_FILE_SIZE`, respecting part size and row-group ratio dyncfgs.
A `CopyToResponse` is returned to the controller with the number of rows written once the upload completes.
