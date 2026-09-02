---
source: src/compute/src/render/sinks.rs
revision: d457e6c14b
---

# mz-compute::render::sinks

Entry point for rendering sink dataflow fragments; dispatches on `ComputeSinkConnection` variant to render either a `Subscribe`, `MaterializedView`, `CopyToS3Oneshot`, or `MetricSink` sink.
Extracts the ok and err collections from the source `CollectionBundle`: if a raw collection is present it is used directly; otherwise an identity `MapFilterProject` is applied to an arrangement to recover one.
Attaches dataflow-error logging, applies `expire_collection_at` if a replica expiration is set, enforces `non_null_assertions` via a `map_fallible` operator that injects null errors, and calls `distinct_errs_collection` on the error stream for `MaterializedView` sinks to normalize error multiplicity before persistence.
The `StartSignal` is forwarded to the sink-specific `SinkRender::render_sink` implementation, which uses it to delay writes until the dataflow has fully hydrated.
