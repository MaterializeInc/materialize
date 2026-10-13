---
source: src/repr/src/explain/tracing.rs
revision: 8941c49828
---

# mz-repr::explain::tracing

Provides `PlanTrace`, a `tracing::Subscriber` layer that captures intermediate query plan stages emitted via `tracing::trace!` spans, enabling `EXPLAIN` to show the plan at each optimizer pass when the `tracing` feature is enabled.
`PlanTrace::new` accepts an `Option<SmallVec<[&'static str; 4]>>` filter of plain path strings; when present, only spans whose path appears in the filter are collected. Callers that filter by `NamedPlan` convert the named plans to their path strings before constructing the trace.
