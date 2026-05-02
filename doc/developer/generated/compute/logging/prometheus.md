---
source: src/compute/src/logging/prometheus.rs
revision: b0fa98e931
---

# mz-compute::logging::prometheus

Logging dataflow fragment that exposes Prometheus metrics gathered from the process-global `MetricsRegistry` as the `ComputeLog::PrometheusMetrics` introspection source.

A custom source operator periodically scrapes the registry (one worker per process, controlled by `COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL`), flattens metric families into a `BTreeMap<SnapshotKey, SnapshotValue>` keyed by `(metric_name, sorted_label_pairs)`, diffs the new snapshot against the previous one, and emits packed `(Row, Row)` key/value pairs using `PermutedRowPacker`. The capability advances at the logging interval rate even when scrapes happen less frequently.

The output stream is arranged into a `RowRowSpine` trace and returned as a `LogCollection` for inclusion in the combined logging dataflow.
