---
source: src/compute-types/src/plan/reduce.rs
revision: 4267863081
---

# compute-types::plan::reduce

Plans aggregation operators by partitioning aggregations into three categories with different incremental maintenance strategies: `AccumulablePlan` (Abelian-group aggregations such as `sum`), `HierarchicalPlan` (associative aggregations such as `min`/`max`, computed in a tree for sub-linear maintenance), and `BasicPlan` (all others, maintained via Differential's `reduce`).
`ReducePlan` combines these into a single enum; `KeyValPlan` describes how rows are split into key and value columns before reduction.
Monotonic inputs can use specialised `MonotonicPlan` variants that avoid retraction overhead.
