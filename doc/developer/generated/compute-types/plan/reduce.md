---
source: src/compute-types/src/plan/reduce.rs
revision: 5680493e7d
---

# compute-types::plan::reduce

Plans aggregation operators by partitioning aggregations into three categories with different incremental maintenance strategies: `AccumulablePlan` (Abelian-group aggregations such as `sum`), `HierarchicalPlan` (associative aggregations such as `min`/`max`, computed in a tree for sub-linear maintenance), and `BasicPlan` (all others, maintained via Differential's `reduce`).
`ReducePlan` combines these into a single enum; `KeyValPlan` describes how rows are split into key and value columns before reduction.
Monotonic inputs can use specialised `MonotonicPlan` variants that avoid retraction overhead.
`BasicPlan` has two variants: `Single` (wraps `SingleBasicPlan`, which carries the aggregate expression and a `fused_unnest_list` flag for fusing a `FlatMap UnnestList` into the reduction) and `Multiple` (a vector of aggregate expressions combined in a collation step).
A `CollationPlan` merges results from multiple sub-reduction types into a single output arrangement using the `aggregate_types` index.
