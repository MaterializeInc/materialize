---
source: src/storage-operators/src/oneshot_source/aws_source.rs
revision: 8089fa9c25
---

# storage-operators::oneshot_source::aws_source

Implements `OneshotSource` for AWS S3 as `AwsS3Source`, with associated `S3Object` and `S3Checksum` types.
`AwsS3Source` lazily initializes an S3 client (via `OnceLock`) from an `AwsConnection` and supports listing objects under a bucket/prefix and streaming object bytes over an optional byte range; a `use_checksum` flag controls whether `ResponseChecksumValidation::WhenRequired` is applied to the S3 client configuration, allowing interoperability with non-AWS endpoints (e.g. GCS) that do not support S3 checksum headers on range requests.
`S3Object` implements `OneshotObject`, providing name, key path, and size metadata extracted from S3 list responses.
