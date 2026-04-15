---
source: src/storage/src/render/sinks.rs
revision: b0fa98e931
---

# mz-storage::render::sinks

Implements `render_sink`, which reads a persist shard via `persist_source`, zips the output into `DiffPair` records using `combine_at_timestamp`, and dispatches to the appropriate sink implementation (Kafka or Iceberg) through the `SinkRender` trait.
Returns a health-status stream and lifecycle tokens.
