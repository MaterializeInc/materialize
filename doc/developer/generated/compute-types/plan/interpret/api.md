---
source: src/compute-types/src/plan/interpret/api.rs
revision: b55d3dee25
---

# compute-types::plan::interpret::api

Defines the `Interpreter` trait, an abstract-interpreter / tagless-final interface over `Plan` nodes.
Each `Plan` variant (constant, get, mfp, flat-map, join, reduce, top-k, negate, threshold, union, arrange-by) corresponds to a method returning an associated `Domain` value, allowing callers to implement analysis passes without pattern matching on the plan tree.
`InterpreterContext` (aliased as `Context`) maps `LocalId` bindings to `ContextEntry` values, each annotated with an `is_rec` flag to distinguish recursive from non-recursive bindings.
`Fold` performs an immutable bottom-up traversal; `FoldMut` performs the same traversal with the ability to mutate plan nodes via a user-supplied action callback.
Both `Fold` and `FoldMut` handle `Let` and `LetRec` bindings (including fixed-point iteration with a cap of `MAX_LET_REC_ITERATIONS`) internally.
`BoundedLattice` extends `differential_dataflow::Lattice` with explicit top/bottom elements, required for fixed-point iteration over recursive bindings.
