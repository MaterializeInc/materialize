---
source: src/catalog/src/durable/metrics.rs
revision: b644395dd7
---

# catalog::durable::metrics

Defines `Metrics`, a Prometheus metrics struct for the durable catalog layer.
Tracks transaction starts and commits, commit latency (`transaction_commit_latency_seconds` histogram), snapshot latency (`snapshot_latency_seconds` histogram), sync counts and sync latency (`sync_latency_seconds` histogram), per-collection entry gauges, ID allocation latency, snapshot consolidation counts, and maximum snapshot entry size. All three latency fields are `Histogram` (with `histogram_seconds_buckets` bucket sets) rather than cumulative counters, so per-operation distributions are observable.
