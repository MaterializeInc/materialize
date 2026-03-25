---
source: src/sql/src/plan/transform_ast.rs
revision: ddc1ff8d2d
---

# mz-sql::plan::transform_ast

Applies SQL-level rewrites to the AST before planning: `FuncRewriter` rewrites functions like `mod`, `nullif`, `avg`, variance/stddev, and others into canonical forms; `Desugarer` expands syntactic sugar (e.g. `EXISTS`, lateral joins, implicit `GROUP BY`, `VALUES` with subqueries).
The public entry point `transform` applies both passes in sequence.
