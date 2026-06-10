---
source: src/transform/src/reduce_reduction.rs
revision: dab94d5cce
---

# mz-transform::reduce_reduction

Implements `ReduceReduction`, which decomposes a `Reduce` containing aggregates of multiple `ReductionType` categories into separate per-type reduces joined back together.
This decomposition enables more efficient execution because each type of reduction (basic, hierarchical, collation) is computed separately and can be arranged independently.
