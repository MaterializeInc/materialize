---
source: src/compute/src/metrics.rs
revision: 39de039eca
---

# mz-compute::metrics

Defines `ComputeMetrics` (process-level) and `WorkerMetrics` (per-worker) Prometheus metric structs for the compute replica.
Tracks arrangement maintenance time, timely step durations, persist peek and stashed peek latencies, command handling durations, index peek latencies broken down by phase (`seek_fulfillment`, `error_scan`, `cursor_setup`, `row_iteration`, `result_sort`, `frontier_check`, `row_collection`, and total), row-count histograms for index peek row iteration (`row_iteration_rows`) and result sorting (`result_sort_rows`), reconciliation outcomes, replica expiration timestamps, collection hydration counts, and subscribe snapshot optimizations.
Index peek row-count histograms use exponential buckets starting at 1 with a factor of 2 across 25 steps, with an explicit zero bucket prepended, so both empty and large result sets are captured accurately.
`CollectionMetrics` wraps `WorkerMetrics` to track the hydration state of a single collection and decrements the collection count gauge on drop.
A `workload_class` label is optionally injected into all metrics via a registry post-processor.
