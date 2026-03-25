---
source: src/sql/src/lib.rs
revision: 48175dc930
---

# mz-sql

Translates SQL statements into dataflow plans through two sequential phases: **purification** (async, inlines external state) and **planning** (pure, converts AST to `Plan`).

Module structure:
* `ast` — re-exports `mz_sql_parser::ast` + catalog rename/rewrite transforms
* `catalog` — `SessionCatalog` trait (the planner's interface to catalog state)
* `names` — all structured name types and the `Aug` resolved-AST info type
* `normalize` — AST→Rust type normalization and `generate_extracted_config!` macro
* `func` — built-in function and operator resolution
* `parse` — thin re-export of `mz_sql_parser::parser`
* `pure` — async purification pipeline (Kafka, Postgres, MySQL, SQL Server, load generators, Iceberg)
* `plan` — `Plan` enum, all plan types, and the full planning pipeline (query, HIR, lowering, statement handlers)
* `rbac` — role-based access control checks
* `session` — session/system variable infrastructure, user/role definitions, session metadata trait
* `kafka_util`, `iceberg` — connector-specific `WITH`-option extraction
* `optimizer_metrics` — Prometheus metrics for optimization latency

Key dependencies: `mz-expr`, `mz-repr`, `mz-sql-parser`, `mz-catalog` (via the `SessionCatalog` trait), `mz-storage-types`, `mz-adapter-types`.
Primary downstream consumer: `mz-adapter`.
