---
source: src/expr/src/scalar.rs
revision: cdb5859050
---

# mz-expr::scalar

Defines `MirScalarExpr`, the central scalar expression type in Materialize's MIR (Mid-level Intermediate Representation).
`MirScalarExpr` is a recursive enum with variants `Column` (carrying an optional name via `TreatAsEqual<Option<Arc<str>>>`), `Literal`, `CallUnmaterializable`, `CallUnary`, `CallBinary`, `CallVariadic`, and `If`; it implements `VisitChildren` enabling the generic traversal infrastructure from `visit.rs`.
Also defines `EvalError` (the comprehensive error enum for runtime scalar evaluation failures, including `InvalidCatalogJson` for malformed catalog JSON, `MultipleRowsFromSubquery` for subqueries returning more than one row, and `NegativeRowsFromSubquery` for subqueries returning a negative row count) and associated type-inference and simplification methods on `MirScalarExpr`.
Child modules: `func` provides all the concrete function implementations; `like_pattern` provides LIKE matching logic.
