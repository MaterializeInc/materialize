---
source: src/expr/src/scalar.rs
revision: 9c1e2767b0
---

# mz-expr::scalar

Defines `MirScalarExpr`, the central scalar expression type in Materialize's MIR (Mid-level Intermediate Representation).
`MirScalarExpr` is a recursive enum with variants `Column`, `Literal`, `CallUnmaterializable`, `CallUnary`, `CallBinary`, `CallVariadic`, and `If`; it implements `VisitChildren` enabling the generic traversal infrastructure from `visit.rs`.
Also defines `EvalError` (the comprehensive error enum for runtime scalar evaluation failures), `AggregateFunc`, `AggregateExpr`, `ColumnOrder`, and associated type-inference and simplification methods on `MirScalarExpr`.
Child modules `func` and `like_pattern` provide all the concrete function implementations and LIKE matching logic.
