---
source: src/transform/src/movement/projection_pushdown.rs
revision: 31a56678e7
---

# mz-transform::movement::projection_pushdown

Implements `ProjectionPushdown`, which pushes column-removal projections as far down the tree as possible to reduce the width of data flowing through operators.
Projections are pushed down; permutations are pushed when convenient; column repetitions are not pushed.
The transform can optionally skip `Join` operators (controlled by `include_joins`, configurable via `skip_joins()`) and is safe to apply across view boundaries in a dataflow.
When a `FlatMap(generate_series(...))` produces values that are not demanded by the outer projection, `collapse_unused_generate_series` rewrites the node into a `RepeatRowNonNegative` that encodes the series cardinality as a diff, avoiding per-row enumeration. With all-literal arguments the cardinality is computed exactly in i128 at rewrite time; with column-valued bounds and a non-zero literal step, arithmetic is synthesized in i64 (for |step|==1) or numeric (for |step|>=2) to stay safe against overflow. The rewrite is declined when the count would exceed i64 or when `func` is `TableFunc::GenerateSeriesUnoptimized`.
