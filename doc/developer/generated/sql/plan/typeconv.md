---
source: src/sql/src/plan/typeconv.rs
revision: 47bd985ddd
---

# mz-sql::plan::typeconv

Maintains the catalog of valid implicit, assignment, and explicit casts between `SqlScalarType`s and implements type coercion for the planner.
Key exports: `plan_cast` (inserts a cast expression given a `CastContext`), `typeconv_table` (the static cast matrix), and `coerce_homogeneous_exprs` (finds the best common type for a set of expressions).
Used extensively by `plan::query` and `func` for argument type checking and implicit conversion.
