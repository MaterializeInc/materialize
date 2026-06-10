---
source: src/sql/src/optimizer_metrics.rs
revision: 7340287c14
---

# mz-sql::optimizer_metrics

Defines `OptimizerMetrics`, which registers and updates Prometheus metrics for the SQL optimizer: end-to-end optimization latency histograms, outer-join lowering case counters, and per-transform hit/total counters.
Emits a warning log when end-to-end optimization time exceeds a configurable threshold, including per-transform timing breakdown.
