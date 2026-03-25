---
source: src/adapter/src/explain/mir.rs
revision: c0a1c584d1
---

# adapter::explain::mir

Implements `Explain` for `MirRelationExpr` and `DataflowDescription<OptimizedMirRelationExpr>` via the `Explainable` newtype, supporting both the `EXPLAIN MIR` and `EXPLAIN PHYSICAL PLAN` stages.
An alternate implementation is provided here rather than in the `expr` crate because the coordinator needs to attach `DataflowMetainfo` and optimizer notices from the adapter's optimization pipeline.
