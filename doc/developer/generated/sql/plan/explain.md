---
source: src/sql/src/plan/explain.rs
revision: 4152ace543
---

# mz-sql::plan::explain

Implements the `Explain` trait for `HirRelationExpr`, enabling `EXPLAIN` output of HIR plans in text and JSON formats.
Before rendering, it optionally normalizes nested subqueries into `Let` blocks via `normalize_subqueries`; the `text` submodule handles the concrete text layout.
`id_gen` scans both `Let` and `LetRec` bindings for their `LocalId`s, preventing `EXPLAIN RAW PLAN` from generating new `LocalId`s that collide with existing ones when the expression contains `LetRec`.
