---
source: src/repr/src/explain.rs
revision: 52af3ba2a1
---

# mz-repr::explain

Defines the `Explain` trait and associated types (`ExplainFormat`, `ExplainConfig`, `ExplainError`) that any explainable subject implements to support `EXPLAIN` statements.
Submodules provide format-specific rendering: `text` for human-readable output, `json` for machine-readable JSON, `dot` for Graphviz, and `tracing` for capturing intermediate plan stages across optimizer passes.
`ExplainConfig` controls which annotations to include (timing, costs, cardinalities, etc.) and is threaded through the optimizer pipeline.
