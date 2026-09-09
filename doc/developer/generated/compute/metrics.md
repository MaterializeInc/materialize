---
source: src/compute/src/metrics.rs
revision: 3fd105cb51
---

# mz-compute::metrics

Defines `ComputeMetrics` (process-level) and `WorkerMetrics` (per-worker) Prometheus metric structs for the compute replica.
Tracks arrangement maintenance time, timely step durations, persist peek latencies, command handling durations, index peek latencies broken down by phase (`seek_fulfillment`, `error_scan`, `cursor_setup`, `row_iteration`, `result_sort`, `frontier_check`, `row_collection`, and total), row-count histograms for index peek row iteration (`row_iteration_rows`) and result sorting (`result_sort_rows`), reconciliation outcomes, replica expiration timestamps, collection hydration counts, and subscribe snapshot optimizations. The `mz_stashed_peek_seconds` histogram has been removed; offloaded peek timing is now covered by `mz_index_peek_offload_seconds`.
New index peek walk metrics track the substrate a walk ended on (`mz_index_peek_walks_total` with `inline` and `offloaded` label values), whether a walk answered from the stash (`mz_index_peek_stashed_total`), the offload permit queue depth (`mz_index_peek_permit_queue_depth`), how long an offloaded walk waited for its permit (`mz_index_peek_permit_wait_seconds`), and how long an offloaded walk was away from the worker including permit wait (`mz_index_peek_offload_seconds`). Both substrate counters of `mz_index_peek_walks_total` are resolved when worker metrics are built, so they start at zero rather than being absent.
Index peek row-count histograms use exponential buckets starting at 1 with a factor of 2 across 25 steps, with an explicit zero bucket prepended, so both empty and large result sets are captured accurately.
`CollectionMetrics` wraps `WorkerMetrics` to track the hydration state of a single collection and decrements the collection count gauge on drop.
A `workload_class` label is optionally injected into all metrics via a registry post-processor.
