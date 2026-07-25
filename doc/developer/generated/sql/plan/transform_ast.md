---
source: src/sql/src/plan/transform_ast.rs
revision: 434f6b82c0
---

# mz-sql::plan::transform_ast

Applies SQL-level rewrites to the AST before planning: `FuncRewriter` rewrites functions like `mod`, `nullif`, `avg`, variance/stddev, and others into canonical forms; `Desugarer` expands syntactic sugar (e.g. `EXISTS`, lateral joins, implicit `GROUP BY`, `VALUES` with subqueries).
The public entry point `transform` applies both passes in sequence.
Variance and standard-deviation rewrites with `DISTINCT` split the `sum(DISTINCT x²)` component into two sign-class aggregates (`sum(DISTINCT CASE WHEN x >= 0 THEN x² END)` and `sum(DISTINCT CASE WHEN x < 0 THEN x² END)`) to ensure deduplication happens on `x`, not on `x²`. Squaring is not injective over all reals (e.g. `-2` and `2` both square to `4`), so a single `sum(DISTINCT x²)` would incorrectly collapse values that differ only in sign.
