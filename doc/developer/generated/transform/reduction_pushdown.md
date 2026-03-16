---
source: src/transform/src/reduction_pushdown.rs
revision: 23126118ae
---

# mz-transform::reduction_pushdown

Implements `ReductionPushdown`, which attempts to push a `Reduce` operator down through a `Join` when the conditions of Galindo-Legaria and Joshi (2001) are met.
In the streaming context, pushing reductions reduces data skew in join arrangements and can allow the join to reuse the reduced arrangement.
The implementation adjusts the second join input to a count-based reduction over the join columns when the key condition cannot be satisfied directly.
