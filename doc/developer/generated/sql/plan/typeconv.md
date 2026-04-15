---
source: src/sql/src/plan/typeconv.rs
revision: 51c628afa3
---

# mz-sql::plan::typeconv

Maintains the catalog of valid implicit, assignment, and explicit casts between `SqlScalarType`s and implements type coercion for the planner.
Key exports: `plan_cast` (inserts a cast expression given a `CastContext`), `guess_best_common_type` (finds the best common type for a set of types), and `plan_coerce` / `plan_hypothetical_cast` for coercion paths.
Used extensively by `plan::query` and `func` for argument type checking and implicit conversion.
