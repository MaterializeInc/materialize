---
source: src/compute/src/metrics.rs
revision: 03de4421ba
---

# mz-compute::metrics

Defines `ComputeMetrics` (process-level) and `WorkerMetrics` (per-worker) Prometheus metric structs for the compute replica.
Tracks arrangement maintenance time, timely step durations, persist peek and stashed peek latencies, command handling durations, index peek latencies broken down by phase (`seek_fulfillment`, `error_scan`, `cursor_setup`, `row_iteration`, `result_sort`, `frontier_check`, `row_collection`, and total), reconciliation outcomes, replica expiration timestamps, collection hydration counts, and subscribe snapshot optimizations.
`CollectionMetrics` wraps `WorkerMetrics` to track the hydration state of a single collection and decrements the collection count gauge on drop.
A `workload_class` label is optionally injected into all metrics via a registry post-processor.
