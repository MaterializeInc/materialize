---
source: src/transform/src/will_distinct.rs
revision: b9af1dbdd8
---

# mz-transform::will_distinct

Implements `WillDistinct`, which propagates a `distinct_by` bit downward through a `MirRelationExpr` tree.
The bit indicates that a downstream operator will mask record multiplicity magnitudes, so sub-expressions are permitted to freely change magnitudes as long as they do not change the polarity (positive, negative, or zero) of any row's multiplicity.
This permission allows redundant `Distinct` (`Reduce` with no aggregates) operators to be removed when they are shadowed by an outer `Distinct`.
A `Distinct` node sets `distinct_by = true` for its input. A `TopK` node with `limit == 1` and `offset == 0` also introduces the permission (when `enable_will_distinct_propagation` is active), because it retains only one row per group regardless of input multiplicities.
The bit propagates unconditionally through sign-preserving operators (`Map`, `Filter`, `FlatMap`, `Threshold`, `Negate`). It propagates through `Project` and `Union` only when the `NonNegative` analysis confirms that no sign-changing cancellations can occur; otherwise the bit is reset to `false`. `Join` always resets the bit to avoid interfering with Semijoin elision.
The `enable_will_distinct_propagation` feature flag (from `OptimizerFeatures`) gates the generalized propagation through `TopK` and the sign-preserving operators; without it only basic `Distinct`-over-`Union` patterns are handled.
