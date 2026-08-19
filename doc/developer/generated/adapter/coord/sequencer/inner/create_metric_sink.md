---
source: src/adapter/src/coord/sequencer/inner/create_metric_sink.rs
revision: 39dcae2fba
---

# adapter::coord::sequencer::inner::create_metric_sink

Implements `CREATE METRIC SINK` sequencing.
`Coordinator::sequence_create_metric_sink` allocates a user id, constructs a `CatalogItem::MetricSink` from the plan, and commits it via `catalog_transact`. No dataflow is optimized or shipped: the catalog item records the definition for future use and survives restarts because `create_sql` is re-parsed on boot.

`ensure_metric_sink_prefix_is_free` enforces a prefix-free uniqueness constraint on all metric sinks bound to the target cluster. The published metric name is `prefix + metric_name`, so two prefixes where one is a prefix of the other would publish colliding names; Prometheus silently merges same-named families. The check is performed at sequencing time rather than at plan time, because planning is not serialized against catalog writes: two concurrent `CREATE METRIC SINK` statements can plan against the same state, but the coordinator sequences one statement at a time and nothing commits between this check and `catalog_transact`. A sink already holding the same item name is skipped, since the create is then a no-op (`IF NOT EXISTS`) or an "already exists" error.

`IF NOT EXISTS` is handled by matching `ErrorKind::Sql(CatalogError::ItemAlreadyExists)` from the catalog transaction and adding an `AdapterNotice::ObjectAlreadyExists` notice rather than returning an error.
