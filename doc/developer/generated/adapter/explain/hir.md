---
source: src/adapter/src/explain/hir.rs
revision: 09152d2db8
---

# adapter::explain::hir

Implements `Explain` for the High-level Intermediate Representation (`HirRelationExpr`) wrapped in `Explainable`, producing text or JSON renderings of the HIR before lowering to MIR.
This module provides the `EXPLAIN HIR` stage of the explain pipeline.
