---
source: src/sql/src/plan/explain.rs
revision: fc2aaf02e7
---

# mz-sql::plan::explain

Implements the `Explain` trait for `HirRelationExpr`, enabling `EXPLAIN` output of HIR plans in text and JSON formats.
Before rendering, it optionally normalizes nested subqueries into `Let` blocks via `normalize_subqueries`; the `text` submodule handles the concrete text layout.
