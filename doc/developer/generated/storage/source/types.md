---
source: src/storage/src/source/types.rs
revision: 5427dc5764
---

# mz-storage::source::types

Defines the core traits and types for the source ingestion framework.
`SourceRender` is the primary trait that all source connections implement; it specifies an associated `Time` timestamp type and a `render` method that produces per-export data collections, health streams, probe streams, and lifecycle tokens.
Also defines `SourceMessage`, `SourceOutput`, `DecodeResult`, `ProgressStatisticsUpdate`, `Probe`, `SignaledFuture`, `StackedCollection`, and `FuelSize` used throughout the source pipeline.
`StackedCollection` is a type alias for a `Collection` backed by `Vec<(D, T, Diff)>` containers.
`FuelSize` is a trait for heap-size estimation; it is implemented for `SourceMessage`, `Row`, `Vec<u8>`, `bytes::Bytes`, `Result<T, E>`, and various stack-only types, and is used by source operators to pass an explicit size to `give_fueled` calls.
`SourceMessage::byte_len()` returns the combined heap size of the key, value, and metadata rows.
`SignaledFuture` wraps an async future with a `Semaphore`-based busy signal so that the operator only polls when the system grants capacity.
