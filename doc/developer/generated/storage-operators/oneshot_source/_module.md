---
source: src/storage-operators/src/oneshot_source.rs
revision: ec53bee597
---

# storage-operators::oneshot_source

Defines the `OneshotSource` and `OneshotFormat` traits plus the five-stage dataflow (discover → split → fetch → decode → stage) used by `COPY FROM` ingestion.
Key types exposed to callers are the `OneshotSource`, `OneshotFormat`, `OneshotObject`, and `StorageErrorX`/`StorageErrorXKind` error types; concrete implementations live in the `aws_source`, `http_source`, `csv`, and `parquet` submodules.
Internal `SourceKind`, `FormatKind`, `ObjectKind`, and `ChecksumKind` enums dispatch over the concrete implementations without requiring trait objects.
When constructing an `AwsS3Source`, checksum validation is conditionally disabled for non-AWS (e.g. GCS) endpoints combined with the Parquet format, since range requests to those endpoints are incompatible with S3 checksum headers.
In the decode stage (`render_decode_chunk`), a `RowArena` is scoped to each decoded chunk and cleared per row so it holds at most one row's worth of MFP-evaluation temporaries at a time, rather than accumulating allocations across the entire source lifetime.
