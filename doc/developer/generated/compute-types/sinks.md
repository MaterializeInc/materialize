---
source: src/compute-types/src/sinks.rs
revision: 2982634c0d
---

# compute-types::sinks

Defines `ComputeSinkDesc<S, T>` and `ComputeSinkConnection<S>`, the descriptor types for compute dataflow sinks.
The four connection variants are: `Subscribe` (streaming query output), `MaterializedView` (persist-backed MV), `ContinualTask` (continual-task sink), and `CopyToS3Oneshot` (one-shot COPY TO S3).
`SubscribeSinkConnection` carries an `output` field (`Vec<ColumnOrder>`) that specifies the ordering for rows emitted by the subscribe.
`MaterializedViewSinkConnection` and `ContinualTaskConnection` carry a `storage_metadata` field that is filled in by the storage/persist layer.
