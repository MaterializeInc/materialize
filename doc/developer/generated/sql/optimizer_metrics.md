---
source: src/sql/src/optimizer_metrics.rs
revision: e1c1ea9d2e
---

# mz-sql::optimizer_metrics

Defines `OptimizerMetrics`, which registers and updates Prometheus metrics for the SQL optimizer: end-to-end optimization latency histograms, outer-join lowering case counters, and per-transform hit/total counters.
Emits a warning log when end-to-end optimization time exceeds a configurable threshold, including per-transform timing breakdown.
The threshold is stored as an `Arc<AtomicU64>` (nanoseconds) so that runtime changes propagate to all clones, including the long-lived copies held by the coordinator and `PeekClient`. Use `set_e2e_optimization_time_log_threshold(threshold: Duration)` to update it atomically via `store(..., Ordering::Relaxed)`. Setting the threshold to zero disables the warning.
