---
source: src/aws-util/src/s3_uploader.rs
revision: 2a6ac3ab4c
---

# mz-aws-util::s3_uploader

Implements `S3MultiPartUploader`, a stateful async helper that streams data to S3 using the S3 multipart upload API without requiring the caller to know the total size in advance.
Data is accumulated in a `BytesMut` buffer and flushed as individual parts (each uploaded concurrently via Tokio tasks) once the configured `part_size_limit` is reached; `finish` completes the upload and returns a `CompletedUpload` summary.
`S3MultiPartUploaderConfig` validates part and file size limits against the S3 constraints (5 MiB min part, 5 GiB max part, 5 TiB max object, 10 000 max parts), and `S3MultiPartUploadError` covers all failure modes.
This module is only compiled when the `s3` feature is enabled.
