# Task 2b report: scalar absorption rule (dropped-operand gated)

## Status: DONE

## What was implemented

Added `absorb_and_or` to `src/transform/src/eqsat/scalar/rules.rs`, registered in
`rules()` immediately after `factor_and_or`. It is the gated counterpart to 2a's
factoring, the second half of the CLU-137 split.

### The rewrite
`a ∨ (a ∧ c) → a` and the dual `a ∧ (a ∨ c) → a`, generalized over inner-sets:
in an outer And/Or, an operand `Q` whose inner-set (by canonical class id) is a
proper SUPERSET of another operand `P`'s inner-set is redundant and is dropped.
Non-destructive: it unions the outer call of the kept operands into the source
class; if exactly one operand remains it unions that class directly. One `Q` is
dropped per fire, saturation removes further redundant operands.

### The gate (on the dropped extras only)
Fires only when every id in `inner-set(Q) \ inner-set(P)` (the extra inner
operands dropped with `Q`) has `could_error == false`. The retained operand `P`
is deliberately NOT gated, it stays in the result so its own error still
surfaces. This is the precise narrowing 2a/2b share: gate only the operands whose
error behavior the rewrite changes, never reduce's over-broad whole-expression
`could_error`. Witness for why the gate is needed: `c0 ∨ (c0 ∧ r)` with `r`
erroring, at row `c0=null, r=err` the input is `err` while the unsound `c0` would
be `null`.

### Shared helper
Extracted `inner_sets(eg, operands, inner_func)` (the per-operand inner-set
computation) and refactored `factor_and_or` to call it. Behavior of 2a is
unchanged (confirmed by its tests still passing).

## Tests
`cargo nextest run -p mz-transform`: 268 passed, 1 skipped. Scalar eqsat subset:
70 passed (66 prior + 4 new). `cargo check`/`clippy -p mz-transform --tests`
clean, `bin/fmt` applied.

Four new tests: soundness over the cube (both duals); firing (absorbs to the
subset operand `c0`, both duals); gate-ALLOWS-retained-error (`g ∨ (g ∧ c1)` with
`g` erroring but retained, absorbs to `g`, differential over the error rows);
gate-BLOCKS-dropped-extra-error (`c0 ∨ (c0 ∧ r)` with `r` erroring, must not
absorb, witness row asserts input errors while unsound `c0` would be null).

## One prior test necessarily changed (NOT strictly 66-unchanged)
`test_factor_and_or_absorption_not_fired_here` asserted that
`(c0∧c1)∨(c0∧c1∧c2)` round-trips UNCHANGED (no absorption). That was only true
while absorption was unimplemented. The empty-residual case is exactly what 2b
absorbs (extra `c2` is error-free, so it collapses to `c0∧c1`), which the 2a test
comment itself anticipated ("that is the absorption rule's job"). I renamed it to
`test_factor_hands_empty_residual_to_absorption` and flipped the assertion: it
now verifies factoring declines the empty-residual case and absorption collapses
it to `c0∧c1`. This is a required consequence of 2b, not a regression. Net count
is still 66 prior tests (65 unchanged + 1 repurposed) + 4 new = 70.

## Deferred
The optional second gate disjunct "retained operand `P` cannot be null" (which
would make absorption sound even when the dropped extras can error) needs a
nullability analysis the scalar engine lacks. Deferred, noted in the doc comment,
matching 2a's deferral of the dual "common factor cannot be null" disjunct.

## Concerns
None on correctness. The only judgment call was repurposing the one 2a guard
test above; the differential test (300 random terms over a div/overflow cube) now
exercises factoring AND absorption together and passes, which is the strongest
end-to-end check.
