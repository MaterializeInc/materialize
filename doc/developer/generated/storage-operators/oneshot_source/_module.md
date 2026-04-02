---
source: src/storage-operators/src/oneshot_source.rs
revision: 8089fa9c25
---

# storage-operators::oneshot_source

Defines the `OneshotSource` and `OneshotFormat` traits plus the five-stage dataflow (discover → split → fetch → decode → stage) used by `COPY FROM` ingestion.
Key types exposed to callers are the `OneshotSource`, `OneshotFormat`, `OneshotObject`, and `StorageErrorX`/`StorageErrorXKind` error types; concrete implementations live in the `aws_source`, `http_source`, `csv`, and `parquet` submodules.
Internal `SourceKind`, `FormatKind`, `ObjectKind`, and `ChecksumKind` enums dispatch over the concrete implementations without requiring trait objects.
When constructing an `AwsS3Source`, checksum validation is conditionally disabled for non-AWS (e.g. GCS) endpoints combined with the Parquet format, since range requests to those endpoints are incompatible with S3 checksum headers.
