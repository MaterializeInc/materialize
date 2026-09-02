---
source: src/sql/src/optimizer_metrics.rs
revision: 43a933b189
---

# mz-sql::optimizer_metrics

Defines `OptimizerMetrics`, which registers and updates Prometheus metrics for the SQL optimizer: end-to-end optimization latency histograms, outer-join lowering case counters, per-transform hit/total counters, and MIR-to-LIR lowering metrics.
Emits a warning log when end-to-end optimization time exceeds a configurable threshold, including per-transform timing breakdown.
The threshold is stored as an `Arc<AtomicU64>` (nanoseconds) so that runtime changes propagate to all clones, including the long-lived copies held by the coordinator and `PeekClient`. Use `set_e2e_optimization_time_log_threshold(threshold: Duration)` to update it atomically via `store(..., Ordering::Relaxed)`. Setting the threshold to zero disables the warning.
`OptimizerMetrics` holds a `LoweringMetrics` (from `mz_compute_types::plan`) registered into the same `MetricsRegistry`. The `lowering()` accessor returns a reference to it for passing to `LirRelationExpr::finalize_dataflow`.
