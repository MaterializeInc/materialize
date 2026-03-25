---
source: src/transform/src/movement.rs
revision: 82048ca795
---

# mz-transform::movement

Groups transforms that move operators up (lifting) or down (pushdown) the expression tree.
Exports `ProjectionLifting` and `ProjectionPushdown`; both can serve as normalization steps or stand-alone optimizations.
