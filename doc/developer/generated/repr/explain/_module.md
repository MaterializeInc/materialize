---
source: src/repr/src/explain.rs
revision: 40e5dd1af8
---

# mz-repr::explain

Defines the `Explain` trait and associated types (`ExplainFormat`, `ExplainConfig`, `ExplainError`) that any explainable subject implements to support `EXPLAIN` statements.
Submodules provide format-specific rendering: `text` for human-readable output, `json` for machine-readable JSON, `dot` for Graphviz, and `tracing` (feature-gated) for capturing intermediate plan stages across optimizer passes.
`ExplainConfig` controls which annotations to include (timing, costs, cardinalities, etc.) and is threaded through the optimizer pipeline.
Imports `OptimizerFeatureOverrides` and the dual-type system types (`ReprColumnType`, `ReprScalarType`, `SqlColumnType`, `SqlScalarType`) for rendering typed plan information.
`ScalarOps` is a trait with two methods — `match_col_ref` (returns the referenced column index if the expression is a column reference) and `references` (returns true if the expression references a given column); `usize` implements `ScalarOps` directly (treating it as a bare column reference).
