---
source: src/sql/src/plan/hir.rs
revision: cc24ca4d96
---

# mz-sql::plan::hir

Defines HIR (High-level Intermediate Representation), the plan IR produced by the SQL planner before decorrelation.
`HirRelationExpr` and `HirScalarExpr` mirror MIR but additionally allow correlated subqueries, lateral references, and column references with nonzero level (outer-relation columns).
Key types include `ColumnRef` (a leveled column reference), `JoinKind` (inner/left/right/full), `AggregateExpr`, `WindowExpr`, and `CoercibleScalarExpr` (a scalar that may still need type coercion).
`could_run_expensive_function` detects potentially expensive expressions by checking for scalar function calls, `CallTable`/`Reduce` operators, and conservatively returns `true` on `RecursionLimitError`.
The module also exposes `lower()` to convert HIR to MIR by delegating to `plan::lowering`.
