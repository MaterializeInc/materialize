---
source: src/transform/src/compound/union.rs
revision: 261d61df83
---

# mz-transform::compound::union

Implements `UnionNegateFusion`, which flattens nested `Union` operators and pushes `Negate` operators inward through unions so that all branches of a single `Union` are at the same level.
When a `Negate` wraps a `Union`, the negation is distributed to each branch, enabling further cancellations and subsequent `Negate` fusion.
This is a structural cleanup that enables other optimizations such as `UnionBranchCancellation`.
