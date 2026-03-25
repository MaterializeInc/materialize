---
source: src/storage/src/source/types.rs
revision: e79a6d96d9
---

# mz-storage::source::types

Defines the core traits and types for the source ingestion framework.
`SourceRender` is the primary trait that all source connections implement; it specifies an associated `Time` timestamp type and a `render` method that produces per-export data collections, health streams, probe streams, and lifecycle tokens.
Also defines `SourceMessage`, `SourceOutput`, `DecodeResult`, `ProgressStatisticsUpdate`, `Probe`, `SignaledFuture`, and `StackedCollection` used throughout the source pipeline.
