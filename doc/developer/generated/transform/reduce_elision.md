---
source: src/transform/src/reduce_elision.rs
revision: 52af3ba2a1
---

# mz-transform::reduce_elision

Implements `ReduceElision`, which removes a `Reduce` operator and replaces it with a `Map` when the input already has unique keys that subsume the reduce's group keys, making the aggregation redundant.
It uses the `UniqueKeys` and `ReprRelationType` analyses to determine when this replacement is safe.
