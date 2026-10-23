---
source: src/compute-types/src/sinks.rs
revision: 41e1741ca3
---

# compute-types::sinks

Defines `ComputeSinkDesc<S>` and `ComputeSinkConnection<S>`, the descriptor types for compute dataflow sinks.
`ComputeSinkDesc` carries a `from_arrangement` field: `None` means the renderer consumes the unarranged collection, and `Some((key, mfp))` names an arrangement of `from` by its key together with an `MfpPlan` that reconstructs full rows of `from_desc` from the arrangement's key and value. It is chosen during LIR lowering and is `None` before lowering runs.
The four connection variants are: `Subscribe` (streaming query output), `MaterializedView` (persist-backed MV), `CopyToS3Oneshot` (one-shot COPY TO S3), and `MetricSink` (writes rows into the in-process Prometheus metrics registry).
`SubscribeSinkConnection` carries an `output` field (`Vec<ColumnOrder>`) that specifies the ordering for rows emitted by the subscribe.
`MaterializedViewSinkConnection` carries a `storage_metadata` field that is filled in by the storage/persist layer.
`MetricSinkConnection` carries a `label: String` field used as the value of the `sink` const label on the sink's companion health gauges. A user-created sink passes its `GlobalId` (durable); a coordinator-installed curated sink passes a stable definition name so the label survives reboots even though the sink's `GlobalId` is transient. The sink does not write to persist.
