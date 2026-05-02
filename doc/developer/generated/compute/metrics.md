---
source: src/compute/src/metrics.rs
revision: 11109fe8ed
---

# mz-compute::metrics

Defines `ComputeMetrics` (process-level) and `WorkerMetrics` (per-worker) Prometheus metric structs for the compute replica.
Tracks arrangement maintenance time, timely step durations, peek latencies broken down by phase (seek, cursor setup, row iteration, sort, etc.), reconciliation outcomes, replica expiration timestamps, collection hydration counts, and subscribe snapshot optimizations.
`CollectionMetrics` wraps `WorkerMetrics` to track the hydration state of a single collection and decrements the collection count gauge on drop.
A `workload_class` label is optionally injected into all metrics via a registry post-processor.
