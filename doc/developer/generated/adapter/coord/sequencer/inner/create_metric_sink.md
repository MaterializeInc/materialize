---
source: src/adapter/src/coord/sequencer/inner/create_metric_sink.rs
revision: a702b8be70
---

# adapter::coord::sequencer::inner::create_metric_sink

Implements `CREATE METRIC SINK` sequencing via the `Staged` pipeline (`CreateMetricSinkStage`), analogously to `CREATE INDEX`.
`sequence_create_metric_sink` enters at `create_metric_sink_validate`, which constructs a `PlanValidity` and returns `CreateMetricSinkStage::Optimize`. The optimize stage runs off the coordinator thread via `spawn_blocking`: it allocates durable (`item_id`, `global_id`) and transient (`view_id`) IDs, builds an `optimize::metric_sink::Optimizer`, and runs the two-phase MIR and LIR optimization. The finish stage runs on the coordinator thread: it calls `ensure_metric_sink_prefix_is_free` (the authoritative prefix-free uniqueness check), commits the `CatalogItem::MetricSink` via `catalog_transact_with_side_effects`, saves the optimized and physical plans, persists the dataflow metainfo, and ships the dataflow via `ship_new_dataflow`. Optimizer notices are rendered before the catalog transaction so the new sink's `global_id` resolves to its intended name.

`ensure_metric_sink_prefix_is_free` enforces a prefix-free uniqueness constraint on all metric sinks bound to the target cluster. The published metric name is `prefix + metric_name`, so two prefixes where one is a prefix of the other would publish colliding names; Prometheus silently merges same-named families. The check is performed in the finish stage on the coordinator thread, not in the optimize stage, because the optimize stage runs off-thread and another sink could commit between the two stages. A sink already holding the same item name is skipped, since the create is then a no-op (`IF NOT EXISTS`) or an "already exists" error.

`IF NOT EXISTS` is handled by matching `ErrorKind::Sql(CatalogError::ItemAlreadyExists)` from the catalog transaction and adding an `AdapterNotice::ObjectAlreadyExists` notice rather than returning an error.
