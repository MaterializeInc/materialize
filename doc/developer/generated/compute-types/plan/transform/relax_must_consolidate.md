---
source: src/compute-types/src/plan/transform/relax_must_consolidate.rs
revision: e926ec3a86
---

# compute-types::plan::transform::relax_must_consolidate

Implements `RelaxMustConsolidate`, a `BottomUpTransform` that uses the `SingleTimeMonotonic` interpreter to identify plan nodes whose input is physically monotonic and clears the `must_consolidate` flag from monotonic `ReducePlan::Hierarchical(MonotonicPlan)`, `TopKPlan::MonotonicTop1`, and `TopKPlan::MonotonicTopK` variants.
This transform runs as part of `LirRelationExpr::finalize_dataflow` after all other lowering steps, via `LirRelationExpr::refine_single_time_consolidation`. The `action` callback matches on `LirRelationNode` variants instead of the former `PlanNode` variants.
