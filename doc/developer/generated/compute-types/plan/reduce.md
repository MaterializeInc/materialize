---
source: src/compute-types/src/plan/reduce.rs
revision: 822256a77d
---

# compute-types::plan::reduce

Plans aggregation operators by partitioning aggregations into three categories with different incremental maintenance strategies: `AccumulablePlan` (Abelian-group aggregations such as `sum`), `HierarchicalPlan` (associative aggregations such as `min`/`max`, computed in a tree for sub-linear maintenance), and `BasicPlan` (all others, maintained via Differential's `reduce`).
`ReducePlan` combines these into a single enum; `KeyValPlan` describes how rows are split into key and value columns before reduction. `KeyValPlan` stores `key_plan: SafeMfpPlan<LirScalarExpr>` and `val_plan: SafeMfpPlan<LirScalarExpr>`.
Aggregate expressions within reduce plans are represented as `LirAggregateExpr` (a stable, serializable struct with `func: AggregateFunc`, `expr: LirScalarExpr`, and `distinct: bool`) rather than `mz_expr::AggregateExpr` (which embeds `MirScalarExpr` and cannot accommodate type parameters due to `MzReflect` derivation). `LirAggregateExpr::from_mir` converts from `AggregateExpr`, panicking on unmaterializable functions. `LirAggregateExpr::is_count_asterisk` detects `COUNT(*)` by checking `func == Count`, `!distinct`, and `expr.is_literal_true()`. `AccumulablePlan`, `SingleBasicPlan`, and the `BasicPlan::Multiple` variant all store `LirAggregateExpr` values.
Monotonic inputs can use specialised `MonotonicPlan` variants that avoid retraction overhead.
`BasicPlan` has two variants: `Single` (wraps `SingleBasicPlan`, which carries the aggregate expression and a `fused_unnest_list` flag for fusing a `FlatMap UnnestList` into the reduction) and `Multiple` (a vector of aggregate expressions combined in a collation step).
A `CollationPlan` merges results from multiple sub-reduction types into a single output arrangement using the `aggregate_types` index.
