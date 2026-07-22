---
source: src/sql/src/plan/lowering.rs
revision: 3d7eb1c1da
---

# mz-sql::plan::lowering

Implements decorrelation — the translation of `HirRelationExpr` to `MirRelationExpr`.
The core algorithm maintains a `ColumnMap` that tracks outer-relation column positions and an `outer` relation; as it traverses nested subqueries it lifts correlated references by cross-joining the subquery against the outer relation and aggregating appropriately.
The `variadic_left` submodule provides a specialized optimization for stacks of left joins, and `transform_hir` passes are applied before lowering begins.
`Config` carries `enable_fixed_correlated_cte_lowering: bool`. When true, the `branch` function uses `visit_post` (which descends into scalar subqueries) to discover all CTE references inside the inner expression, and emits CTE outer columns at the head of the branch key so that the `Get` arm's reconciliation join can match them positionally. When false, the deprecated `visit` path is used instead, which misses CTE references nested inside scalar subqueries and can produce incorrect correlations (SQL-349).
