---
source: src/sql/src/plan/query.rs
revision: 8be80d79b9
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
`INTERSECT` planning calls `HirRelationExpr::relation_node_count` on both inputs and places the smaller one on the left. Because the planner duplicates the left input, a left-deep INTERSECT chain without this swap would produce exponential plan size; with it, plan size is bounded at O(n^log2(3)).
`MAX_TYPE_NESTING_DEPTH` (128) and `MAX_TYPE_RESOLUTION_NODES` (100,000) are constants bounding custom type resolution. `scalar_type_from_catalog` delegates to `scalar_type_from_catalog_inner` via `TypeResolutionBudget`, rejecting custom types that are nested deeper than `MAX_TYPE_NESTING_DEPTH` or that require more than `MAX_TYPE_RESOLUTION_NODES` sub-type resolutions, preventing stack overflow and memory exhaustion from pathological types.
When planning a `SELECT` with table functions in the SELECT list alongside a `GROUP BY`, aggregates, or `HAVING`, table functions are joined on top of the reduce (after GROUP BY and aggregation), not before it. The planner speculatively joins the table function before the reduce so that its columns are in scope during SELECT list expansion and GROUP BY planning, then checks whether any group key or aggregate actually references the table function columns. If none do, the join is rolled back, the reduce is applied on the pre-join relation, and Step 8.5 re-plans the table function join on top of the reduce output, rebinding column references to the reduced relation.
