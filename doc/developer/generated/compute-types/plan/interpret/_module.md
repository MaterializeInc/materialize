---
source: src/compute-types/src/plan/interpret.rs
revision: e926ec3a86
---

# compute-types::plan::interpret

Re-exports the public `Interpreter` / `BoundedLattice` / `Context` / `FoldMut` API from `api` and the `PhysicallyMonotonic` / `SingleTimeMonotonic` concrete interpreter from `physically_monotonic`.
Both sub-modules are kept private to enforce the separation between the API surface and its implementations. The module provides abstract interpretation utilities for `LirRelationExpr` structures.
