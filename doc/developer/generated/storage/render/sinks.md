---
source: src/storage/src/render/sinks.rs
revision: 85fe1c5a51
---

# mz-storage::render::sinks

Implements `render_sink`, which reads a persist shard via `persist_source`, arranges the output by sink key via `arrange_sink_input` (producing a `SinkBatchStream`), and dispatches to the appropriate sink implementation (Kafka or Iceberg) through the `SinkRender` trait.
`SinkBatchStream` is the arranged stream of batches with the trace reader dropped so the spine's compaction frontiers can advance freely; individual sinks walk each batch's cursor via `mz_interchange::envelopes::for_each_diff_pair`.
Also defines `PkViolationWarner`, a rate-limited detector that warns when a non-synthetic `(key, timestamp)` group contains more than one `DiffPair`, and the `SinkRender` trait itself.
Returns a health-status stream and lifecycle tokens.
