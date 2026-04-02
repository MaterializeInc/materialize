---
source: src/aws-util/src/lib.rs
revision: 80414ab91c
---

# mz-aws-util

Provides Materialize-specific wrappers and utilities for the AWS SDK, centering on opinionated client construction and S3 multipart uploads.

## Module structure

* `lib.rs` — crate root; exports `defaults()` (a `ConfigLoader` with latest behavior version, using the AWS SDK's default rustls HTTP client).
* `s3` — S3 client construction, object listing, and a `futures::Stream` adapter for `ByteStream` (feature-gated on `s3`).
* `s3_uploader` — `S3MultiPartUploader`, a streaming multipart-upload helper with configurable part/file size limits (feature-gated on `s3`).

## Key dependencies

* `aws-config`, `aws-sdk-s3`, `aws-smithy-types`, `aws-types` — AWS SDK components.
* `mz-ore` — task spawning (`mz_ore::task::spawn`) and error formatting utilities.

## Downstream consumers

Used by any Materialize component that reads from or writes to S3, including the COPY TO S3 export path and source/sink connectors that interact with S3-compatible storage.
