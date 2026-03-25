---
source: src/transform/src/canonicalization.rs
revision: 52af3ba2a1
---

# mz-transform::canonicalization

Brings relation expressions into a canonical structural form by simplifying enclosed scalar expressions and converting relation operators into equivalent but simpler forms.
Exports `FlatMapElimination`, `ProjectionExtraction`, `TopKElision`, and `ReduceScalars`.
`ReduceScalars` is defined directly in the module file; it visits every AST node and calls `MirScalarExpr::reduce` on each scalar expression using type information from the `ReprRelationType` analysis.
The three submodule transforms and `ReduceScalars` are composed together in the `NormalizeOps` transform defined at the crate root.
