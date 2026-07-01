# Task 1b report: scalar rule infrastructure + constant folding

## Status

DONE_WITH_CONCERNS

## Commit

`158e9db7a1`

## Test summary

30 tests pass: 8 new rule tests (`eqsat::scalar::rules::tests::*`) + 12
pre-existing analysis tests + 10 pre-existing Phase 0 round-trip tests.

## What was done

* Created `src/transform/src/eqsat/scalar/rules.rs` with the `Rule` type,
  the `rules()` registry, and the `const_fold` rule.
* Updated `egraph.rs`: removed `#[allow(dead_code)]` on `MATCH_LIMIT`,
  added `class_ids()`, wired the collect/apply split into `saturate`,
  and fixed a latent bug in the rebuild analysis fixpoint.
* Updated `scalar.rs`: added `pub mod rules`.

## Concern: rebuild analysis fixpoint fix

The brief did not anticipate a latent bug in `rebuild()`'s analysis fixpoint.
The original code broke out of the per-class node loop on the FIRST node
with an unresolvable child, leaving the entire class without an analysis even
when other nodes in the class (e.g. a Literal) were independently resolvable.

Constant folding exposes this: when `neg(0)` folds to `Literal(0)` and the
two classes are unioned, the merged class contains both `Literal(0)` and
`CallUnary{NegInt64, self}` (the call's child becomes the merged class itself
after canonicalization). The old fixpoint loop could not resolve the class
because the self-referential CallUnary node blocked the loop before the
Literal node was processed.

The fix: skip individual unresolvable nodes and merge analyses from the nodes
that ARE ready. A class that contains a Literal node (even if it also contains
a self-referential call) can be resolved using the Literal's analysis.

Soundness argument: the class IS the literal after folding. The cyclic call
node is a structural artifact of canonicalization that does not affect
extraction (the literal has lower cost). Using only the Literal node's analysis
may under-approximate `could_error` for the cyclic call node, but:
1. `const_fold` reads only `literal`, not `could_error`, from child analyses.
2. No other Phase 1 rules read `could_error` from a guard.
3. The class IS semantically a literal, so `could_error: false` (for a valid
   literal) is the correct analysis.

For Phase 1c/1d (boolean/null rules that gate on `could_error`), the fix
remains sound: a class that is provably a literal cannot error, so
`could_error: false` is correct even if the cyclic call node (which IS the
same literal semantically) might nominally "could error" due to the function.

The fix was validated by the `test_const_fold_differential` test which
exercises `neg(0)` (the concrete triggering case: `-0 = 0` hashes to the
existing `Literal(0)` class, causing the self-referential merge).
