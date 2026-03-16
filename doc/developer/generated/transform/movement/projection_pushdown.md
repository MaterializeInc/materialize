---
source: src/transform/src/movement/projection_pushdown.rs
revision: 52af3ba2a1
---

# mz-transform::movement::projection_pushdown

Implements `ProjectionPushdown`, which pushes column-removal projections as far down the tree as possible to reduce the width of data flowing through operators.
Projections are pushed down; permutations are pushed when convenient; column repetitions are not pushed.
The transform can optionally skip `Join` operators (controlled by `include_joins`, configurable via `skip_joins()`) and is safe to apply across view boundaries in a dataflow.
