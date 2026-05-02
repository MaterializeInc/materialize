---
source: src/storage-operators/src/s3_oneshot_sink/pgcopy.rs
revision: 5680493e7d
---

# storage-operators::s3_oneshot_sink::pgcopy

Implements `CopyToS3Uploader` for the PostgreSQL `COPY` text/CSV format as `PgCopyUploader`.
Rows are encoded via `mz_pgcopy::encode_copy_format` and buffered in an `S3MultiPartUploader`; when the file size limit is reached a new multipart upload is started automatically.
