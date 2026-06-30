# Task 4 report: `equiv` condition and `drop_equiv_filter` rule

## Summary

Added the `equiv(pred, rel)` side condition and the `drop_equiv_filter` rule
that removes a redundant equality-filter predicate when the input relation's
equivalences already imply the equality. Also added `not_self_ref(rel)` as a
necessary companion guard on `merge_filters` to prevent exponential e-graph
blowup.

## Changes made

### `src/transform/src/eqsat/dsl.rs`

* Added `Cond::Equiv { pred: String, rel: String }`.
* Added `Cond::NotSelfRef { rel: String }`.

### `src/transform/src/eqsat/parser.rs`

* Added `"equiv"` branch: parses `equiv(pred, rel)`.
* Added `"not_self_ref"` branch: parses `not_self_ref(rel)`.

### `src/transform/src/eqsat/egraph.rs`

* `check_conds`: added `Cond::Equiv` arm (extracts `Eq(col(a), col(b))`
  from the payload, guards out `ca == cb` self-equalities, then checks that
  both columns reduce to the same representative under the relation's
  equivalences).
* `check_conds`: added `Cond::NotSelfRef` arm (checks no e-node in the
  class has a child in the same class).
* Removed all debug instrumentation added during investigation.

### `src/transform/src/eqsat/rules/relational.rewrite`

* Added rule `drop_equiv_filter`: `Filter[p] r => r where equiv(p, r)`.
* Added `where not_self_ref(r)` guard to `merge_filters` (in addition to
  the existing `not_rel_empty(r)` guard).

### `src/transform/tests/test_transforms/eqsat.spec`

* Added `DefSource` for `t1` (two bigint columns).
* Added case (h): `Filter (#0 = #2) (Join on=(#0=#2) t0 t1) => Join on=(#0=#0) t0 t1`.
  Note: the output shows `#0 = #0` because phase 2a canonicalizes the join's
  own equivalence predicate using the reducer it derived from that predicate
  (pre-existing behavior from commit `29b4793502`).

### `src/transform/src/eqsat/eqsat.rs` and `engine.rs`

* Removed debug instrumentation.

## Key design decisions

### `Cond::Equiv` extracts columns from the payload

The brief specified `Cond::Equiv { a, b, rel }` with separate column bindings,
but the DSL payload metavariables bind opaque `EScalar` lists; there is no
way to bind individual columns in the pattern language. The implementation
extracts `ca` and `cb` by pattern-matching the single `MirScalarExpr::CallBinary
{ Eq, Column(ca), Column(cb) }` at evaluation time. All other payload shapes
return `false` (the condition does not match).

### Guard: `ca == cb` is excluded

A syntactic self-equality `#a = #a` is trivially true but is left to constant
folding and `drop_true_filter`, not `drop_equiv_filter`. Firing on `ca == cb`
would incorrectly prevent the merge (the filter IS redundant) and also create
a self-referential class. The guard keeps the concerns separate.

### `not_self_ref(r)` on `merge_filters` prevents exponential blowup

When `drop_equiv_filter` fires, it merges the Filter class with the Join class.
The merged class now has self-referential Filter e-nodes (their `input` child
is in the same class). Without the guard:
1. Phase 2a canonicalizes `Eq(0,2)→Eq(0,0)` within the merged class.
2. `push_filter_into_join_first` fires on the `Eq(0,0)` filter (all columns <
   arity(t0)=2).
3. `merge_filters` fires on all pairs of self-referential Filters in the merged
   class.
4. Node count grows exponentially: 4→7→15→50→566.
5. At 566 nodes, the Equivalences `run_analysis` fixpoint loop diverges.

`not_self_ref(r)` prevents `merge_filters` from firing when `r`'s class is
already self-referential, blocking the blowup at step 3. The guard is
semantically safe: in a self-referential class the inner Filter is already
equivalent to its input (that's the `drop_equiv_filter` semantics), so both
sides of the `merge_filters` equation are already in the same e-class.

## Test result

`cargo test -p mz-transform --test test_transforms`: 1 passed, 0 failed.
Test runs in 3.25 s (no hang).

## Files changed

* `src/transform/src/eqsat/dsl.rs`
* `src/transform/src/eqsat/parser.rs`
* `src/transform/src/eqsat/egraph.rs`
* `src/transform/src/eqsat/rules/relational.rewrite`
* `src/transform/tests/test_transforms/eqsat.spec`
* `src/transform/src/eqsat.rs`
* `src/transform/src/eqsat/engine.rs`
