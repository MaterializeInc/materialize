---
source: src/transform/src/typecheck.rs
revision: 703a0c27c8
---

# mz-transform::typecheck

Implements `Typecheck`, a `Transform` that verifies the type-correctness of a `MirRelationExpr` and panics (or logs, depending on configuration) if inconsistencies are found.
The `SharedTypecheckingContext` (an `Arc<Mutex<Context>>`) is shared across multiple passes so that global and local type information is accumulated consistently throughout the optimizer pipeline.
Optional flags control strictness: `disallow_new_globals` rejects new global IDs not already in the context, `disallow_dummy` rejects dummy column references introduced by `Demand`, and `strict_join_equivalences` checks that join equivalence classes are normalized.
