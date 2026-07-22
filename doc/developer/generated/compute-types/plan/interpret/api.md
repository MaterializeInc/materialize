---
source: src/compute-types/src/plan/interpret/api.rs
revision: e926ec3a86
---

# compute-types::plan::interpret::api

Defines the `Interpreter` trait, an abstract-interpreter / tagless-final interface over `LirRelationExpr` nodes.
Each `LirRelationExpr` variant (constant, get, mfp, flat-map, join, reduce, top-k, negate, threshold, union, arrange-by) corresponds to a method returning an associated `Domain` value, allowing callers to implement analysis passes without pattern matching on the plan tree. The `Interpreter` trait is no longer generic over a timestamp type; `constant` uses `Timestamp` directly. Method signatures for `mfp`, `flat_map`, `reduce`, and `arrange_by` take `LirScalarExpr`-based types (`MfpPlan<LirScalarExpr>`, `SafeMfpPlan<LirScalarExpr>`) instead of `MapFilterProject` / `MirScalarExpr`. The `flat_map` and `reduce` methods also carry the `input_key` parameter before the `input` domain value.
`InterpreterContext` (aliased as `Context`) maps `LocalId` bindings to `ContextEntry` values, each annotated with an `is_rec` flag to distinguish recursive from non-recursive bindings.
`Fold` performs an immutable bottom-up traversal; `FoldMut` performs the same traversal with the ability to mutate plan nodes via a user-supplied action callback.
Both `Fold` and `FoldMut` handle `Let` and `LetRec` bindings (including fixed-point iteration with a cap of `MAX_LET_REC_ITERATIONS`) internally.
`BoundedLattice` extends `differential_dataflow::Lattice` with explicit top/bottom elements, required for fixed-point iteration over recursive bindings.
