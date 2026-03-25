---
source: src/catalog/src/durable/metrics.rs
revision: 6ef7bdfb88
---

# catalog::durable::metrics

Defines `Metrics`, a Prometheus metrics struct for the durable catalog layer.
Tracks transaction starts and commits, commit and snapshot latency histograms, sync counts and latency, per-collection entry gauges, and ID allocation latency.
