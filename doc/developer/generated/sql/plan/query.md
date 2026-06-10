---
source: src/sql/src/plan/query.rs
revision: fc2aaf02e7
---

# mz-sql::plan::query

The largest module in the planner: converts SQL `Query` AST nodes into `HirRelationExpr` and SQL scalar expressions into `HirScalarExpr`.
Entry points include `plan_root_query` (for top-level `SELECT`), `plan_expr` (scalar expressions), `plan_as_of_or_up_to` (temporal qualifiers), and many `plan_*` helpers for joins, subqueries, `GROUP BY`, `ORDER BY`, window functions, etc.
`ExprContext` and `QueryContext` carry planner state (catalog, scope, available columns, parameter types) through the recursive descent; `NameManager` tracks expression aliases for CSE.
The `repeat_row` function (identified via the `is_repeat_row` helper, which matches against `mz_catalog.repeat_row`) is explicitly disallowed in `ROWS FROM` expressions and in `SELECT` clauses that contain multiple table functions.
`invent_column_name` returns `Result<Option<ColumnName>, PlanError>` so that internal invariant violations (e.g., a function call whose name did not resolve to a catalog item) propagate as `PlanError::Internal` rather than panicking.
The private `humanize_or_debug` helper renders a `ResolvedItemName` for use in error messages, falling back to a debug dump if humanization fails.
When a `JOIN ... USING (col) AS alias` is planned via `plan_using_constraint`, the expression bound to the aliased column is chosen based on join kind: for `INNER` and `LEFT OUTER` joins it uses the LHS column value; for `RIGHT OUTER` joins it uses the RHS column value; and for `FULL OUTER` joins it uses `COALESCE(lhs, rhs)`, matching the semantics of the unqualified join output column.
