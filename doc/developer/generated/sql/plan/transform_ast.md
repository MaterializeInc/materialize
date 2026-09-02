---
source: src/sql/src/plan/transform_ast.rs
revision: d01594110c
---

# mz-sql::plan::transform_ast

Applies SQL-level rewrites to the AST before planning: `FuncRewriter` rewrites functions like `mod`, `nullif`, `avg`, variance/stddev, and others into canonical forms; `Desugarer` expands syntactic sugar (e.g. `EXISTS`, lateral joins, implicit `GROUP BY`, `VALUES` with subqueries).
The public entry point `transform` applies both passes in sequence.
Variance and standard-deviation rewrites with `DISTINCT` split the `sum(DISTINCT x²)` component into two sign-class aggregates (`sum(DISTINCT CASE WHEN x >= 0 THEN x² END)` and `sum(DISTINCT CASE WHEN x < 0 THEN x² END)`) to ensure deduplication happens on `x`, not on `x²`. Squaring is not injective over all reals (e.g. `-2` and `2` both square to `4`), so a single `sum(DISTINCT x²)` would incorrectly collapse values that differ only in sign.
The `ANY`/`ALL` array desugaring rewrites `$expr op ALL ($array_expr)` (and the `ANY` form) into a subquery over `unnest($array_expr)`. When the `enable_any_all_null_array_semantics` feature flag is enabled, the result is wrapped in `CASE WHEN $array_expr IS NULL THEN NULL ELSE ... END`. This guard implements PostgreSQL-compatible semantics: a `NULL` array yields `NULL` rather than the empty-set answer that `unnest` would produce (false for `ANY`, true for `ALL`). The guard is omitted when the flag is off because it defeats the batched HIR lowering that shares a single `unnest` across several `ANY`/`ALL` operands over a non-constant array; with the guard each operand lowers independently and duplicates the `unnest`. In filter context and whenever the array is constant, the guard folds away and plan quality is unchanged.
