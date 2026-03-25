---
source: src/storage-operators/src/oneshot_source.rs
revision: 1f5cd0d026
---

# storage-operators::oneshot_source

Defines the `OneshotSource` and `OneshotFormat` traits plus the five-stage dataflow (discover → split → fetch → decode → stage) used by `COPY FROM` ingestion.
Key types exposed to callers are the `OneshotSource`, `OneshotFormat`, `OneshotObject`, and `StorageErrorX`/`StorageErrorXKind` error types; concrete implementations live in the `aws_source`, `http_source`, `csv`, and `parquet` submodules.
Internal `SourceKind`, `FormatKind`, `ObjectKind`, and `ChecksumKind` enums dispatch over the concrete implementations without requiring trait objects.
