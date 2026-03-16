---
source: src/adapter/src/optimize/view.rs
revision: 52af3ba2a1
---

# adapter::optimize::view

Implements the optimizer pipeline for `CREATE VIEW` as a single HIR-to-MIR stage that produces an `OptimizedMirRelationExpr`.
Views are not lowered to LIR here; the resulting expression is stored in the catalog and incorporated into downstream dataflows at query time.
