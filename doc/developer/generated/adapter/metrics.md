---
source: src/adapter/src/metrics.rs
revision: a1bcaebfe6
---

# adapter::metrics

Registers and vends all Prometheus metrics for the adapter and coordinator.
`Metrics` is the top-level struct holding counters, gauges, and histograms covering query counts, active sessions, subscribe/COPY-TO activity, timestamp determination, statement logging, message handling latency, and more; `SessionMetrics` is a lightweight subset scoped to a single session.
`Metrics` includes a `timestamp_difference_for_bounded_staleness_ms` histogram (per-compute-instance label) that records how much older bounded-staleness timestamps are compared to serializable, measuring the actual staleness incurred. `SessionMetrics` exposes this via `timestamp_difference_for_bounded_staleness_ms`.
The `mz_time_to_first_row_seconds` histogram carries an `application_name` label in addition to `instance_id`, `isolation_level`, and `strategy`.
`Metrics` tracks catalog snapshot cache behavior via `catalog_snapshot_seconds` (a `HistogramVec` labeled by `context`, observed only on cache misses) and `catalog_snapshot_cache` (an `IntCounterVec` labeled by `context` and `result`, counting hits and misses). `catalog_arc_strong_count` and `catalog_arc_weak_count` are `UIntGauge` metrics tracking the number of strong and weak references to the current catalog snapshot `Arc`, respectively.
`Metrics` includes `catalog_transact_seconds` (a `HistogramVec` labeled by `method`) for timing catalog transact methods, `catalog_transact_phase_seconds` (a `HistogramVec` labeled by `phase`) for fine-grained per-phase timing within a catalog transaction (phases overlap and do not sum to `catalog_transact_seconds`), `apply_catalog_implications_seconds` for timing catalog implication application, and `group_commit_catalog_upper_seconds` for timing catalog shard upper advances during group commits and table register/forget operations.
Several public metrics carry `MetricTag` annotations for categorization: `mz_query_total`, `mz_active_sessions`, `mz_active_subscribes`, and `mz_adapter_commands` carry `MetricTag::Environment`.
Helper functions `session_type_label_value`, `statement_type_label_value`, and `subscribe_output_label_value` produce the label strings used for partitioning these metrics.
`Metrics` includes a `subscribe_outputs` `IntCounterVec` (labeled via `subscribe_output_label_value`) counting subscribe output rows; `SessionMetrics` vends per-call counters from it via `Metrics::subscribe_outputs`.
`Metrics` includes `active_internal_subscribes: IntGaugeVec` (labeled by `session_type`) tracking the number of active internal subscribes, used by frontend-sequenced read-then-write operations and coordinator background maintenance. Internal subscribes are not reflected in `active_subscribes` or in the `mz_subscriptions` builtin table.
`Metrics` includes `occ_retry_count: Histogram` recording the number of OCC retry attempts made per frontend read-then-write execution before it either succeeds or exhausts its retry budget.
`Metrics` includes four hydration-history metrics: `hydration_history_mutations: IntCounterVec` (labeled by `operation` and `outcome`) counting collection and retention mutations; `hydration_history_retention_batch_full: IntCounter` counting sweeps whose retention batch was full (repeated increments indicate retention may not be keeping up with its schedule); `hydration_history_rows_affected: IntCounterVec` (labeled by `action`) counting rows changed by maintenance; and `hydration_history_sweep_duration_seconds: Histogram` recording the wall time of a complete collection and retention sweep. None of these carry cluster, replica, or object labels.
