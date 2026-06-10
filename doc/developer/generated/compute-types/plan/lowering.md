---
source: src/compute-types/src/plan/lowering.rs
revision: 92cb1cf559
---

# compute-types::plan::lowering

Implements lowering from `DataflowDescription<OptimizedMirRelationExpr>` (MIR) to `DataflowDescription<Plan>` (LIR) via `Context::lower`.
A `Context` tracks known arrangements keyed by `Id`, tracks which IDs may produce future-stamped updates (from temporal MFPs using `mz_now()`), assigns monotonically increasing `LirId`s to each node, and carries a `LirDebugInfo` for error messages.
`MirRelationExpr` variants (Constant, Get, Let, LetRec, FlatMap, Join, Reduce, TopK, Negate, Threshold, Union, ArrangeBy) are recursively translated to corresponding `PlanNode` variants; join implementation selection consults the `JoinImplementation` annotation (Differential, DeltaQuery, IndexedFilter, or Unimplemented) already placed by the optimizer.
`FlatMap` lowering prefers the unarranged collection when available (columns are already in logical order); when reading from an arrangement, both the table-function argument expressions and the `mfp_after` MFP are permuted to match the arrangement's column order using `permute` on individual expressions and `permute_fn` on the MFP.
A `FlatMap(UnnestList)` atop a `Reduce` for window functions can be fused into the `Reduce` plan directly during lowering, avoiding the need for a separate `FlatMap` stage.
The `enable_reduce_mfp_fusion` feature flag controls whether MFP expressions above a `Reduce` are partially fused into the `Reduce`'s `mfp_after` field.
Lowering sets an `ArrangementStrategy` (`Direct` or `TemporalBucketing`) on `Reduce`, `TopK`, and `Union` nodes based on whether their inputs may carry future-stamped updates. `TemporalBucketing` is chosen when `has_future_updates` is true; the renderer later applies a temporal bucket operator to the relevant input stream, absorbing future updates before they reach an arrangement or consolidation point. For `Union`, the bucketing decision is coupled with the `consolidate_output` flag: per-input strategies are set only for consolidating Unions (those with at least one `Negate` input), since bucketing ahead of a non-consolidating Union adds overhead with no benefit. The `refine_union_negate_consolidation` post-pass has been folded into this lowering step, since bucketing strategy depends on per-input `has_future_updates` information available only at lowering time.
