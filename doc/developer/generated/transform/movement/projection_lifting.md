---
source: src/transform/src/movement/projection_lifting.rs
revision: 52af3ba2a1
---

# mz-transform::movement::projection_lifting

Implements `ProjectionLifting`, which hoists `Project` operators upward through other operators (filters, maps, unions, joins, reductions).
Lifting projections simplifies the operators below them by narrowing the column space they operate on, and prepares for subsequent fusion or pushdown passes.
