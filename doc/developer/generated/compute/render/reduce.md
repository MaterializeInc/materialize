---
source: src/compute/src/render/reduce.rs
revision: 1c55de49eb
---

# mz-compute::render::reduce

Renders all `ReducePlan` variants (accumulable, hierarchical, basic, and collation) into differential dataflow operators.
Accumulable reductions use differential's built-in `reduce` with specialized difference types; hierarchical reductions build a tree of partial aggregations for improved parallelism; basic reductions handle arbitrary `LirAggregateExpr`s.
A `CollationPlan` combines multiple sub-reductions and merges them into the final output arrangement via a multi-input join.
When a `Reduce` node carries `temporal_bucketing_strategy: TemporalBucketing`, the renderer applies a temporal bucket operator to the keyed `(key, val)` stream before it is fed into `render_reduce_plan`. This is done inside `render_reduce` rather than at an upstream `ArrangeBy` site because `Reduce` builds its own internal arrangement via `KeyValPlan`, bypassing `ensure_collections`.
