---
source: src/adapter/src/metrics.rs
revision: dbd2c3fc06
---

# adapter::metrics

Registers and vends all Prometheus metrics for the adapter and coordinator.
`Metrics` is the top-level struct holding counters, gauges, and histograms covering query counts, active sessions, subscribe/COPY-TO activity, timestamp determination, statement logging, message handling latency, and more; `SessionMetrics` is a lightweight subset scoped to a single session.
`Metrics` includes a `timestamp_difference_for_bounded_staleness_ms` histogram (per-compute-instance label) that records how much older bounded-staleness timestamps are compared to serializable, measuring the actual staleness incurred. `SessionMetrics` exposes this via `timestamp_difference_for_bounded_staleness_ms`.
The `mz_time_to_first_row_seconds` histogram carries an `application_name` label in addition to `instance_id`, `isolation_level`, and `strategy`.
`Metrics` tracks catalog snapshot cache behavior via `catalog_snapshot_seconds` (a `HistogramVec` labeled by `context`, observed only on cache misses) and `catalog_snapshot_cache` (an `IntCounterVec` labeled by `context` and `result`, counting hits and misses). `catalog_arc_strong_count` and `catalog_arc_weak_count` are `UIntGauge` metrics tracking the number of strong and weak references to the current catalog snapshot `Arc`, respectively.
Several public metrics carry `MetricTag` annotations for categorization: `mz_query_total`, `mz_active_sessions`, `mz_active_subscribes`, and `mz_adapter_commands` carry `MetricTag::Environment`.
Helper functions `session_type_label_value`, `statement_type_label_value`, and `subscribe_output_label_value` produce the label strings used for partitioning these metrics.
