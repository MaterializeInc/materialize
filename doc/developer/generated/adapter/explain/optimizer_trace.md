---
source: src/adapter/src/explain/optimizer_trace.rs
revision: 8941c49828
---

# adapter::explain::optimizer_trace

Implements `OptimizerTrace`, a tracing subscriber that intercepts optimizer stage spans and captures the intermediate representations (HIR, MIR, `DataflowDescription<LirRelationExpr>`, fast-path plan) emitted during optimization.
When `EXPLAIN ... WITH (...)` specifies a stage, `OptimizerTrace` is installed as a `tracing` layer; after optimization completes, `drain_explainee` retrieves the captured IR for that stage and formats it using the appropriate `Explain` implementation.
`OptimizerTrace::new` converts any `NamedPlan` filter into plain path strings before constructing the underlying `PlanTrace`.
The module-private `used_indexes_for` function looks up the `UsedIndexes` instance that corresponds to a given plan path: `Global` and `Physical` paths resolve to the `Global` traced entry; `FastPath` paths resolve to the `FastPath` traced entry; all others return a default `UsedIndexes`.
