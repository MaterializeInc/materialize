---
source: src/sql/src/plan/hir.rs
revision: fc2aaf02e7
---

# mz-sql::plan::hir

Defines HIR (High-level Intermediate Representation), the plan IR produced by the SQL planner before decorrelation.
`HirRelationExpr` and `HirScalarExpr` mirror MIR but additionally allow correlated subqueries, lateral references, and column references with nonzero level (outer-relation columns).
Key types include `ColumnRef` (a leveled column reference), `JoinKind` (inner/left/right/full), `AggregateExpr`, `WindowExpr`, and `CoercibleScalarExpr` (a scalar that may still need type coercion).
`could_run_expensive_function` detects potentially expensive expressions by checking for unary, binary, variadic, and windowing function calls, `CallTable`/`Reduce` operators, and conservatively returns `true` on `RecursionLimitError`.
`HirScalarExpr` implements `VisitChildren<HirRelationExpr>` to expose its immediate subquery bodies (`Exists`/`Select` arms) without descending further; this is the asymmetric counterpart of `VisitChildren<HirScalarExpr>` on `HirRelationExpr`, which traverses scalars into subqueries at any depth.
HIR is converted to MIR via `HirRelationExpr::lower()`, which is defined in `plan::lowering` as an `impl HirRelationExpr` method.
