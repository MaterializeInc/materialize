---
source: src/sql/src/plan/query.rs
revision: efc8864da8
---

# mz-sql::plan::query

The largest module in the planner: converts SQL `Query` AST nodes into `HirRelationExpr` and SQL scalar expressions into `HirScalarExpr`.
Entry points include `plan_root_query` (for top-level `SELECT`), `plan_expr` (scalar expressions), `plan_as_of_or_up_to` (temporal qualifiers), and many `plan_*` helpers for joins, subqueries, `GROUP BY`, `ORDER BY`, window functions, etc.
`ExprContext` and `QueryContext` carry planner state (catalog, scope, available columns, parameter types) through the recursive descent; `NameManager` tracks expression aliases for CSE.
The `repeat_row` function (identified via the `is_repeat_row` helper, which matches against `mz_catalog.repeat_row`) is explicitly disallowed in `ROWS FROM` expressions and in `SELECT` clauses that contain multiple table functions.
`plan_table_function_internal` returns user-facing errors (via `sql_bail!`) when a table function carries `FILTER`, `OVER`, or `DISTINCT`; previously these were panics.
`invent_column_name` returns `Result<Option<ColumnName>, PlanError>` so that internal invariant violations (e.g., a function call whose name did not resolve to a catalog item) propagate as `PlanError::Internal` rather than panicking.
The private `humanize_or_debug` helper renders a `ResolvedItemName` for use in error messages, falling back to a debug dump if humanization fails.
When a `JOIN ... USING (col) AS alias` is planned via `plan_using_constraint`, the expression bound to the aliased column is chosen based on join kind: for `INNER` and `LEFT OUTER` joins it uses the LHS column value; for `RIGHT OUTER` joins it uses the RHS column value; and for `FULL OUTER` joins it uses `COALESCE(lhs, rhs)`, matching the semantics of the unqualified join output column.
`plan_nested_query` treats a nested query as an unordered relation: its `ORDER BY` is dropped rather than materialized into a `TopK` unless combined with a `LIMIT`/`OFFSET` clause. This diverges from PostgreSQL, where order-sensitive aggregates observe a sorted subquery's output as an executor artifact. Callers that need a specific aggregation order must use the in-aggregate `agg(value ORDER BY ...)` form.
`MAX_TYPE_NESTING_DEPTH` (128) and `MAX_TYPE_RESOLUTION_NODES` (100,000) are constants bounding custom type resolution. `scalar_type_from_catalog` delegates to `scalar_type_from_catalog_inner` via `TypeResolutionBudget`, rejecting custom types that are nested deeper than `MAX_TYPE_NESTING_DEPTH` or that require more than `MAX_TYPE_RESOLUTION_NODES` sub-type resolutions, preventing stack overflow and memory exhaustion from pathological types.
