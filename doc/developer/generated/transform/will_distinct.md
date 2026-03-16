---
source: src/transform/src/will_distinct.rs
revision: ddc1ff8d2d
---

# mz-transform::will_distinct

Implements `WillDistinct`, which propagates the information that a collection will later be subjected to a `Distinct` (group-by with no aggregates) down through `Union` chains.
When a `Union` is wrapped by a `Distinct` and all but one of its branches already perform a `Distinct`, the inner `Distinct` operations can be removed because the outer one subsumes them.
Uses the `NonNegative` analysis to guard correctness.
