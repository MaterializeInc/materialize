---
source: src/ore/src/bytes.rs
revision: ee8f3e1a8d
---

# mz-ore::bytes

Defines `SegmentedBytes`, a cheaply clonable collection of non-contiguous `Bytes` segments backed by a `SmallVec`, and the internal `SegmentedReader` type that wraps it with `io::Read` and `io::Seek` support.

`SegmentedBytes` implements `bytes::Buf` for streaming consumption and, under the `parquet` feature, implements the `parquet::file::reader::ChunkReader` interface for zero-copy reads into Parquet decoders.
The design avoids large contiguous allocations by storing independently owned `Bytes` slices; `SegmentedReader` uses accumulated-length bookkeeping to enable efficient binary-search-based seeking across segments.
