---
source: src/expr/src/scalar.rs
revision: af9155582e
---

# mz-expr::scalar

Defines `MirScalarExpr`, the central scalar expression type in Materialize's MIR (Mid-level Intermediate Representation).
`MirScalarExpr` is a recursive enum with variants `Column`, `Literal`, `CallUnmaterializable`, `CallUnary`, `CallBinary`, `CallVariadic`, and `If`; it implements `VisitChildren` enabling the generic traversal infrastructure from `visit.rs`.
Also defines `EvalError` (the comprehensive error enum for runtime scalar evaluation failures, including `InvalidCatalogJson` for malformed catalog JSON), `AggregateFunc`, `AggregateExpr`, `ColumnOrder`, and associated type-inference and simplification methods on `MirScalarExpr`.
Child modules: `func` provides all the concrete function implementations; `like_pattern` provides LIKE matching logic.
