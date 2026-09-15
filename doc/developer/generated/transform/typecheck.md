---
source: src/transform/src/typecheck.rs
revision: 7038348d4c
---

# mz-transform::typecheck

Implements `Typecheck`, a `Transform` that verifies the type-correctness of a `MirRelationExpr` and panics (or logs, depending on configuration) if inconsistencies are found.
The `SharedTypecheckingContext` (an `Arc<Mutex<Context>>`) is shared across multiple passes so that global and local type information is accumulated consistently throughout the optimizer pipeline.
`TypeError` enumerates the possible inconsistencies: `Unbound` (unbound identifiers), `NoSuchColumn` (invalid column dereference), `MismatchColumn` (single column type mismatch), `MismatchColumns` (relation-level type mismatch), `BadConstantRowLen` (constant row with wrong number of fields), `BadConstantRow` (constant row with fields of wrong type), `BadProject` (projection of a non-existent column), `BadJoinEquivalence` (malformed join equivalence classes), `BadTopKGroupKey` (TopK group key referencing a non-existent column), `BadTopKOrdering` (TopK ordering referencing a non-existent column), `BadLetRecBindings` (malformed LetRec bindings), `Shadowing` (shadowed local identifiers), `Recursion` (recursion depth exceeded), and `DisallowedDummy` (dummy column reference present when disallowed).
Optional flags control strictness: `disallow_new_globals` rejects new global IDs not already in the context, `disallow_dummy` rejects dummy column references, and `strict_join_equivalences` checks that join equivalence classes are normalized.
`typecheck_scalar` walks `MirScalarExpr` iteratively using `Visit::try_visit_post`, accumulating child types in a stack so that each node pops its children's results and pushes its own. This avoids recursion at the scalar level, so scalar expression depth is bounded only by the heap. The `RecursionGuard` on `Typecheck` is used exclusively by the relation-level `typecheck` walk; `Recursion` errors are therefore only reachable through deep relation trees, not deep scalar expressions. `typecheck_aggregate` delegates directly to `typecheck_scalar` without a separate recursion check.
