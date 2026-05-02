---
source: src/compute-types/src/plan/transform/relax_must_consolidate.rs
revision: b55d3dee25
---

# compute-types::plan::transform::relax_must_consolidate

Implements `RelaxMustConsolidate`, a `BottomUpTransform` that uses the `SingleTimeMonotonic` interpreter to identify plan nodes whose input is physically monotonic and clears the `must_consolidate` flag from monotonic `ReducePlan::Hierarchical(MonotonicPlan)`, `TopKPlan::MonotonicTop1`, and `TopKPlan::MonotonicTopK` variants.
This transform runs as part of `Plan::finalize_dataflow` after all other lowering steps, via `Plan::refine_single_time_consolidation`.
