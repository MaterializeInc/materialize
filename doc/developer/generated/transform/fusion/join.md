---
source: src/transform/src/fusion/join.rs
revision: 52af3ba2a1
---

# mz-transform::fusion::join

Implements `Join` fusion: merges chains of binary `Join` operators into a single multiway join, removes unit collections (arity-0 with multiplicity 1) from join inputs, and simplifies joins with zero or one input into identity or empty operations.
Filters on top of nested joins are lifted using `PredicatePushdown` before fusion so the nested joins are accessible.
The `Join::action` function is also called directly by `NormalizeOps`.
