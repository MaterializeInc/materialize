---
source: src/repr/src/explain/tracing.rs
revision: c58b2ebb27
---

# mz-repr::explain::tracing

Provides `PlanTrace`, a `tracing::Subscriber` layer that captures intermediate query plan stages emitted via `tracing::trace!` spans, enabling `EXPLAIN` to show the plan at each optimizer pass when the `tracing` feature is enabled.
