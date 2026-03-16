---
source: src/transform/src/fold_constants.rs
revision: 52af3ba2a1
---

# mz-transform::fold_constants

Implements `FoldConstants`, which replaces operators whose inputs are all constant collections with a single `Constant` expression, effectively evaluating the plan at compile time.
An optional `limit` controls the maximum size of constant expressions that will be inlined; passing `None` disables the limit.
The transform handles all `MirRelationExpr` variants, including `Join`, `Reduce`, `TopK`, `Threshold`, and `FlatMap`, and is designed to be iterated to a fixpoint together with `NormalizeLets` via `fold_constants_fixpoint` defined in `lib.rs`.
