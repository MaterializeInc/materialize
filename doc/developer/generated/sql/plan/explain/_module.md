---
source: src/sql/src/plan/explain.rs
revision: 261d61df83
---

# mz-sql::plan::explain

Provides `EXPLAIN` support for HIR plans: the root file hooks `HirRelationExpr` into the `Explain` trait, and the `text` submodule renders it as indented text.
Both virtual (high-level) and raw syntax modes are supported.
