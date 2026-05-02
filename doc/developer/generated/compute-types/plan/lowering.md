---
source: src/compute-types/src/plan/lowering.rs
revision: b55d3dee25
---

# compute-types::plan::lowering

Implements lowering from `DataflowDescription<OptimizedMirRelationExpr>` (MIR) to `DataflowDescription<Plan>` (LIR) via `Context::lower`.
A `Context` tracks known arrangements keyed by `Id`, assigns monotonically increasing `LirId`s to each node, and carries a `LirDebugInfo` for error messages.
`MirRelationExpr` variants (Constant, Get, Let, LetRec, FlatMap, Join, Reduce, TopK, Negate, Threshold, Union, ArrangeBy) are recursively translated to corresponding `PlanNode` variants; join implementation selection consults the `JoinImplementation` annotation (Differential, DeltaQuery, IndexedFilter, or Unimplemented) already placed by the optimizer.
A `FlatMap(UnnestList)` atop a `Reduce` for window functions can be fused into the `Reduce` plan directly during lowering, avoiding the need for a separate `FlatMap` stage.
The `enable_reduce_mfp_fusion` feature flag controls whether MFP expressions above a `Reduce` are partially fused into the `Reduce`'s `mfp_after` field.
