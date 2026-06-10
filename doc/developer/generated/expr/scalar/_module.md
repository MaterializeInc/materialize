---
source: src/expr/src/scalar.rs
revision: fc2aaf02e7
---

# mz-expr::scalar

Defines `MirScalarExpr`, the central scalar expression type in Materialize's MIR (Mid-level Intermediate Representation).
`MirScalarExpr` is a recursive enum with variants `Column` (carrying an optional name via `TreatAsEqual<Option<Arc<str>>>`), `Literal`, `CallUnmaterializable`, `CallUnary`, `CallBinary`, `CallVariadic`, and `If`; it implements `VisitChildren` enabling the generic traversal infrastructure from `visit.rs`.
Also defines `EvalError` (the comprehensive error enum for runtime scalar evaluation failures, including `InvalidCatalogJson` for malformed catalog JSON, `MultipleRowsFromSubquery` for subqueries returning more than one row, and `NegativeRowsFromSubquery` for subqueries returning a negative row count) and associated type-inference and simplification methods on `MirScalarExpr`.
`MirScalarExpr` implements the `Columns` and `Eval` traits, which are re-exported from this module.
Child modules: `columns` defines the `Columns` trait for abstracting over column-reference operations; `eval` defines the `Eval` trait for abstracting over scalar evaluation; `func` provides all the concrete function implementations; `like_pattern` provides LIKE matching logic; `optimizable` defines the `OptimizableExpr` trait (the bound for the generic `MapFilterProject<E>` parameter); `reduce` (private) holds the implementation of `MirScalarExpr::reduce`.
`OptimizableExpr` is re-exported from this module alongside `Columns` and `Eval`.
