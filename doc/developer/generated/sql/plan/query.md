---
source: src/sql/src/plan/query.rs
revision: 52af3ba2a1
---

# mz-sql::plan::query

The largest module in the planner: converts SQL `Query` AST nodes into `HirRelationExpr` and SQL scalar expressions into `HirScalarExpr`.
Entry points include `plan_root_query` (for top-level `SELECT`), `plan_expr` (scalar expressions), `plan_aggregate` (aggregates), and many `plan_*` helpers for joins, subqueries, `GROUP BY`, `ORDER BY`, window functions, etc.
`ExprContext` and `QueryContext` carry planner state (catalog, scope, available columns, parameter types) through the recursive descent; `NameManager` tracks expression aliases for CSE.
