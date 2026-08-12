---
source: src/compute/src/sink/metric_sink.rs
revision: 94054eb165
---

# mz-compute::sink::metric_sink

Render arm for `MetricSinkConnection`: the compute-side operator that funnels a source collection's rows into the process's Prometheus registry.

## Architecture

The operator routes all data to one worker per process (chosen by hashing the sink's `GlobalId`), accumulates updates into a `SinkState`, and exposes that state to the registry through a `SinkCollector`. `SinkState` is shared between the timely operator (the sole writer) and `SinkCollector::collect` (the reader) via `Arc<Mutex<_>>`. Both sides hold the lock only across a short synchronous section: the operator has no `await` points and the collector clones out only what it needs before releasing the lock.

## Key types

**`SinkState`** — the full working and published state for one metric sink. Incoming updates are buffered by timestamp in `pending_ok`/`pending_err` and folded into `working` only once the combined ok+err frontier has closed that timestamp. `working` holds a signed multiplicity per full row identity (`RowKey`). `published` is rebuilt from the live set of `working` on each healthy activation.

**`RowKey`** — `(metric_name, labels, value_bits, metric_kind, name_valid, help)`. The name and labels lead the tuple so that a `BTreeMap<RowKey, _>` keeps all rows of one `(metric_name, labels)` series adjacent for efficient collision detection.

**`MetricKind`** — `Gauge` or `Counter`, recovered from the `metric_kind` column the planner's `shape_metric_sink_source` already computed (`0` = gauge, `1` = counter).

**`SinkCollector`** — a `prometheus::core::Collector` that exposes six companion gauges (`mz_metric_sink_frontier_ms`, `mz_metric_sink_errors`, `mz_metric_sink_skipped`, `mz_metric_sink_conflicts`, `mz_metric_sink_collisions`, `mz_metric_sink_null_values`) plus the user-defined series built dynamically as `MetricFamily` protos. The companion gauges carry a `sink` const label so each sink instance has distinct `Desc` ids at registration time.

## Collision and conflict semantics

A *collision* is a `(metric_name, labels)` series with more than one distinct live non-null value. The winner is the row with the numerically smallest value, breaking ties by metric kind then help string. A *conflict* is a published series whose `metric_type`/`help` disagree with the family's winning type/help (determined by the lexicographically smallest label vector within the family). A series all of whose live values are null is absent from `published` and counted in `null_values`.

## Publication freeze

While the sink's error count (`errors`) is nonzero, `published` is not rebuilt; it stays at the last healthy snapshot. `working` continues integrating closed timestamps during the freeze so that the next healthy `publish_if_healthy` call reflects everything that happened while frozen.

## Planner contract

The source relation is expected to carry the seven canonical columns produced by `mz_adapter::optimize::metric_sink::shape_metric_sink_source`: `metric_name`, `metric_type`, `labels` (non-null), `value`, `help` (non-null), `metric_kind`, and `name_valid`. `ColumnIndices::resolve` panics if a required column is missing; the SQL planner enforces this contract once the `CREATE METRIC SINK` planning path exists.
