---
source: src/adapter/src/explain/optimizer_trace.rs
revision: e757b4d11b
---

# adapter::explain::optimizer_trace

Implements `OptimizerTrace`, a tracing subscriber that intercepts optimizer stage spans and captures the intermediate representations (HIR, MIR, LIR, fast-path plan) emitted during optimization.
When `EXPLAIN ... WITH (...)` specifies a stage, `OptimizerTrace` is installed as a `tracing` layer; after optimization completes, `drain_explainee` retrieves the captured IR for that stage and formats it using the appropriate `Explain` implementation.
