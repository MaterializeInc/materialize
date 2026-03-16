---
source: src/storage-operators/src/oneshot_source/aws_source.rs
revision: 68b342f74e
---

# storage-operators::oneshot_source::aws_source

Implements `OneshotSource` for AWS S3 as `AwsS3Source`, with associated `S3Object` and `S3Checksum` types.
`AwsS3Source` lazily initializes an S3 client and supports listing objects under a bucket/prefix and streaming object bytes over an optional byte range.
