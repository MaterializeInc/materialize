---
source: src/compute-types/src/plan/transform/relax_must_consolidate.rs
revision: aa395ac946
---

# compute-types::plan::transform::relax_must_consolidate

Implements `RelaxMustConsolidate<T>`, a `BottomUpTransform` that uses the `SingleTimeMonotonic` interpreter to identify plan nodes that are physically monotonic and removes unnecessary `must_consolidate` flags from monotonic `TopKPlan` and `ReducePlan` variants.
This transform runs as part of `Plan::finalize_dataflow` after all other lowering steps.
