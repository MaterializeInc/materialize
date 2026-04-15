---
source: src/compute/src/render/reduce.rs
revision: 52f2de096d
---

# mz-compute::render::reduce

Renders all `ReducePlan` variants (accumulable, hierarchical, basic, and collation) into differential dataflow operators.
Accumulable reductions use differential's built-in `reduce` with specialized difference types; hierarchical reductions build a tree of partial aggregations for improved parallelism; basic reductions handle arbitrary `AggregateExpr`s.
A `CollationPlan` combines multiple sub-reductions and merges them into the final output arrangement via a multi-input join.
