---
source: src/compute-types/src/sinks.rs
revision: 94054eb165
---

# compute-types::sinks

Defines `ComputeSinkDesc<S>` and `ComputeSinkConnection<S>`, the descriptor types for compute dataflow sinks.
The four connection variants are: `Subscribe` (streaming query output), `MaterializedView` (persist-backed MV), `CopyToS3Oneshot` (one-shot COPY TO S3), and `MetricSink` (writes rows into the in-process Prometheus metrics registry).
`SubscribeSinkConnection` carries an `output` field (`Vec<ColumnOrder>`) that specifies the ordering for rows emitted by the subscribe.
`MaterializedViewSinkConnection` carries a `storage_metadata` field that is filled in by the storage/persist layer.
`MetricSinkConnection` carries no payload: the identity of the metric to update is the sink's `GlobalId`, and the sink does not write to persist.
