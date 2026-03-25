---
source: src/transform/src/fusion/reduce.rs
revision: da91c9a7f1
---

# mz-transform::fusion::reduce

Implements `Reduce` fusion: when a `Reduce` sits directly on top of another `Reduce` whose group keys are a superset of the outer group keys and whose aggregates can be simply projected, the two reduces are merged into one.
