---
source: src/sql/src/plan/explain.rs
revision: 4152ace543
---

# mz-sql::plan::explain

Provides `EXPLAIN` support for HIR plans: the root file hooks `HirRelationExpr` into the `Explain` trait, and the `text` submodule renders it as indented text.
Both virtual (high-level) and raw syntax modes are supported.
`id_gen` scans both `Let` and `LetRec` bindings for their `LocalId`s, preventing `EXPLAIN RAW PLAN` from generating new `LocalId`s that collide with existing ones when the expression contains `LetRec`.
