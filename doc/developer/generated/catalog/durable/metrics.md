---
source: src/catalog/src/durable/metrics.rs
revision: 31ebed7760
---

# catalog::durable::metrics

Defines `Metrics`, a Prometheus metrics struct for the durable catalog layer.
Tracks transaction starts and commits, commit and snapshot latency histograms, sync counts and latency, per-collection entry gauges, ID allocation latency, snapshot consolidation counts, and maximum snapshot entry size.
