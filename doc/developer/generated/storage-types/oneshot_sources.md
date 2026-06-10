---
source: src/storage-types/src/oneshot_sources.rs
revision: a375623c5b
---

# storage-types::oneshot_sources

Defines types for one-shot (non-streaming) ingestion requests, used by `COPY FROM` operations.
`OneshotIngestionRequest` specifies the `ContentSource` (HTTP URL or AWS S3 URI), `ContentFormat` (CSV or Parquet), `ContentFilter` (none, explicit file list, or regex), and `ContentShape` (source relation desc plus an MFP to project/filter to the target schema).
`OneshotIngestionDescription` carries the dataflow tokens and result channel back to the caller.
