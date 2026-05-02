---
source: src/adapter/src/explain.rs
revision: e757b4d11b
---

# adapter::explain

Houses the `EXPLAIN` support for all intermediate representations used in the optimization pipeline.
The `Explainable<T>` newtype bridges adapter-local types (HIR, MIR, LIR, fast-path plans, dataflow descriptions) with the generic `mz_repr::explain::Explain` trait, while `explain_dataflow` is a convenience wrapper that builds an `ExplainContext` and renders a complete dataflow plan.
Child modules implement `Explain` for each IR stage (`hir`, `mir`, `lir`, `fast_path`) and provide the tracing-based `OptimizerTrace` capture mechanism and the plan-insights analysis.
