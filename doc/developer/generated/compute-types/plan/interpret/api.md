---
source: src/compute-types/src/plan/interpret/api.rs
revision: 833454ccc9
---

# compute-types::plan::interpret::api

Defines the `Interpreter<T>` trait, an abstract-interpreter / tagless-final interface over `Plan` nodes.
Each `Plan` variant (constant, get, let, mfp, flat-map, join, reduce, top-k, threshold, etc.) corresponds to a method returning an associated `Domain` value, allowing callers to implement analysis passes without pattern matching on the plan tree.
`Context` maps `LocalId` bindings to domain values; `FoldMut` wraps an `Interpreter` to perform a bottom-up traversal and accumulate results on mutable plan nodes.
`BoundedLattice` extends `differential_dataflow::Lattice` with explicit top/bottom elements, required for fixed-point iteration over recursive bindings.
