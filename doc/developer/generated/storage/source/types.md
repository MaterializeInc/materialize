---
source: src/storage/src/source/types.rs
revision: f498b6e141
---

# mz-storage::source::types

Defines the core traits and types for the source ingestion framework.
`SourceRender` is the primary trait that all source connections implement; it specifies an associated `Time` timestamp type and a `render` method that produces per-export data collections, health streams, probe streams, and lifecycle tokens.
Also defines `SourceMessage`, `SourceOutput`, `DecodeResult`, `ProgressStatisticsUpdate`, `Probe`, `SignaledFuture`, and `StackedCollection` used throughout the source pipeline.
`SignaledFuture` wraps an async future with a `Semaphore`-based busy signal so that the operator only polls when the system grants capacity.
