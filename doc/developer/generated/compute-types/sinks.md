---
source: src/compute-types/src/sinks.rs
revision: da6dea38ae
---

# compute-types::sinks

Defines `ComputeSinkDesc<S, T>` and `ComputeSinkConnection<S>`, the descriptor types for compute dataflow sinks.
The four connection variants are: `Subscribe` (streaming query output), `MaterializedView` (persist-backed MV), `ContinualTask` (continual-task sink), and `CopyToS3Oneshot` (one-shot COPY TO S3).
`MaterializedViewSinkConnection` and `ContinualTaskConnection` carry a `storage_metadata` field that is filled in by the storage/persist layer.
