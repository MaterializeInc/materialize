# Task 2a: undistribute factoring rule (residual-error gated)

Phase 2 of the scalar equality-saturation canonicalizer. This adds the
common-factor undistribution rewrite, the structural half of the CLU-137 fix.
It is the hardest rule so far: it matches over a variadic operand list, computes
a common-factor intersection, and builds a nested factored term. Depends on the
1h contract (`fn(eg: &mut ScalarEGraph, &SNode) -> Vec<Id>`, build nested nodes
via `eg.add`).

READ FIRST for the soundness model and the corrected design:
`doc/developer/design/20260625_eqsat_scalar_expressions.md` section "The CLU-137
lesson, stated precisely" (esp. the envelope subsection). Key point: factoring is
NOT unconditionally sound; this task ships the conservative, exact-eval-sound
gate.

## The rewrite (both duals)

```text
undistribute-OR  (outer Or, inner And):  (a∧b)∨(a∧c) → a∧(b∨c)
undistribute-AND (outer And, inner Or):  (a∨b)∧(a∨c) → a∨(b∧c)
```

Generalized to the FULL common-factor intersection across all outer operands
(this is what fixes CLU-137, see below). Source of truth: the `intersection`
path of `MirScalarExpr::undistribute_and_or` (`src/expr/src/scalar.rs:916`,
specifically the `!intersection.is_empty()` branch ~`:960`). The e-graph version
is non-destructive (union, do not mutate) and needs NO fixpoint loop and NO
heuristic choice (reduce's loop and its subset-heuristic exist only because it is
destructive).

SCOPE: implement FULL-INTERSECTION factoring only (factor the operands common to
ALL outer operands). DEFER subset factoring (reduce's `else` branch ~`:978`, the
`indexes_to_undistribute` path) to a later task. Full-intersection is what the
CLU-137 repro needs (all DNF branches share the common conjuncts) and keeps this
task tractable. Note the deferral in your report.

## Why FULL intersection, not one factor at a time

The CLU-137 predicate is `(a∧b∧c∧(s IS NULL)∧T) ∨ (a∧b∧c∧(s='')∧T)` where
`T = mz_now() < timestamp_tz_to_mz_timestamp(#0)` (a FALLIBLE cast, so `T` can
error). Factoring the WHOLE intersection `{a,b,c,T}` at once gives
`a∧b∧c∧T ∧ ((s IS NULL)∨(s=''))`. The residuals are `s IS NULL` and `s=''`,
which CANNOT error, so the gate (below) fires and `T` reaches the top level.

If instead you factored one operand at a time (say `a` alone), the residuals
would still contain `T`, which CAN error, so the gate would block. Full
intersection is what isolates the error-free residuals. Mirror reduce: compute
the intersection of all branches, factor it out in one step.

## The gate (conservative, exact-eval sound)

Factoring `(...)∨(...) → factor ∧ (residual-disjunction)` is unsound under exact
evaluation when the common factor can be NULL and one residual short-circuits
(true) while another residual errors, masking the error on one side but not the
other (see the design note's worked counterexample). The conservative sufficient
gate this task uses:

* FIRE only when EVERY residual operand (every inner-operand class that ends up
  inside the residual disjunction, across all branches) has
  `eg.analysis(id).could_error == false`.

The common factor itself MAY error (that is the whole CLU-137 point, the fallible
cast lives in the factor). Do NOT gate on the factor's `could_error`. This is the
precise narrowing of reduce's over-broad `self.could_error()`.

(A second independently-sound disjunct, "the common factor cannot be null",
needs a nullability analysis we do not have. DEFER it. Note in your report that
adding it would broaden applicability when residuals can error.)

## Keep factoring DISToINCT from absorption (do not reintroduce CLU-137)

Absorption `a∨(a∧c) → a` is a SEPARATE rule (task 2b) with its OWN gate (on the
dropped operand). This task must NOT perform absorption, and must not produce a
form that the Phase 1 rules then collapse INTO an ungated absorption.

The mechanism: absorption is the case where, after removing the intersection, a
branch's residual is EMPTY (the branch WAS exactly the intersection, e.g.
`(a∧b)∨(a∧b∧c)` has intersection `{a,b}` and residuals `{}`,`{c}`). An empty
residual would build `And()` = empty-And, which Phase 1 (`and_or_empty`) turns
into `true`, then `Or(true, ...)` short-circuits, then `And(factor, true)` drops
to `factor` = absorption, UNGATED.

Therefore: FIRE factoring ONLY when EVERY branch has a NON-EMPTY residual (every
outer operand has at least one inner operand not in the intersection). If any
residual is empty, do not fire (2b handles that, gated). This single condition
cleanly separates factoring from absorption.

## The rule, step by step (undistribute-OR; the dual is symmetric via switch_and_or)

Add `factor_and_or` to `rules.rs`, registered in `rules()`. Reuse `is_and_or`,
`switch_and_or`. For `node = CallVariadic { func: outer_func, exprs }` with
`is_and_or(outer_func)`:

1. `inner_func = outer_func.switch_and_or()`.
2. For each outer operand id `o`, compute its inner-operand SET (as canonical
   class ids, `eg.find`): if the operand's class contains a
   `CallVariadic { func: inner_func, exprs: inner }` node, use `{find(e) for e in
   inner}`; otherwise treat the operand as a singleton `{find(o)}` (this is
   reduce's 1-arg wrapping). NOTE a class may hold more than one node; pick the
   `inner_func` variadic node if present (mirror how `not_not` scans
   `eg.nodes`). If a class holds several `inner_func` nodes, using the first is
   fine (they are equal); keep it simple and deterministic.
3. `intersection` = set intersection of all branches' inner sets. If empty, return
   `vec![]` (nothing common).
4. For each branch, `residual_i` = its inner set minus `intersection`. If ANY
   `residual_i` is empty, return `vec![]` (absorption case, leave to 2b).
5. GATE: if ANY id in ANY `residual_i` has `could_error == true`, return
   `vec![]`.
6. Build (read all sets into owned values BEFORE the `eg.add` calls, per 1h borrow
   discipline):
   * For each branch: `branch_residual_id` = if `residual_i` has one element, that
     element id; else `eg.add(CallVariadic { inner_func, residual_i ids })`.
   * `residual_disjunction` = `eg.add(CallVariadic { outer_func, [branch_residual_id ...] })`.
   * `factored` = `eg.add(CallVariadic { inner_func, intersection ids ++ [residual_disjunction] })`.
   * return `vec![factored]`.

Determinism: sort the intersection ids and the residual id lists before building,
so the constructed nodes are stable (hashcons keys on the operand vector). Match
the existing rules' determinism habits.

Termination: the factored form is structurally smaller (fewer total e-nodes) than
the source for a genuine factoring, and `add` hashconses, so re-firing on an
already-factored term produces an existing class and unions nothing new. Confirm
saturation still terminates within the existing bounds (the differential and
`test_fold_terminates`-style checks).

## Tests (in the rules.rs test module, existing style)

1. SOUNDNESS over the cube. `assert_eval_equiv((c0∧c1)∨(c0∧c2), 3)` and the dual
   `assert_eval_equiv((c0∨c1)∧(c0∨c2), 3)`. Residuals are columns (error-free) so
   the rule fires; proves 3VL preservation.
2. FIRING. `canonicalize((c0∧c1)∨(c0∧c2))` should extract `c0∧(c1∨c2)` (the
   factored form is cheaper: ~5 e-nodes vs ~7, so min-cost extraction picks it).
   Assert equality up to commutative operand order (compare as sets if needed,
   like other variadic tests). Dual likewise. This proves the rule fired.
3. FULL-INTERSECTION + ERRORING COMMON FACTOR (the CLU-137 property). Let
   `g = (1 / c0 = 5)` (a non-literal bool that errors at c0==0, modeling the
   fallible cast in the common factor). Build
   `(g ∧ c2) ∨ (g ∧ c3)` with c2,c3 bool columns (error-free residuals). Assert
   it factors to `g ∧ (c2 ∨ c3)` (g, the erroring factor, is pulled out) and run
   a differential `eval(input)` vs `eval(canonicalize(input))` over rows incl.
   `c0 == 0` (g errors) and c2,c3 over {true,false,null}. Proves factoring fires
   with an erroring COMMON FACTOR and stays exact-eval-equal.
4. GATE BLOCKS on erroring RESIDUAL (CLU-137-class soundness). Build
   `(c0 ∧ r) ∨ (c0 ∧ c2)` where `r = (1 / c1 = 5)` errors at c1==0 (an erroring
   RESIDUAL, not the factor). Assert the rule does NOT produce the factored form
   `c0 ∧ (r ∨ c2)` (i.e., canonicalize does not change observable eval): run the
   differential over rows incl. `c0=null`-ish (c0 over {t,f,null}) and `c1==0`,
   and assert exact-eval equality holds (which it must, since blocking keeps the
   original). To make the gate-block meaningful, ALSO assert the unsound factored
   form is NOT the extraction (e.g. the output still has the outer Or shape, or
   structurally != the factored form). The point: if the gate were absent, the
   differential would FAIL at c0=null, r=err, c2=true (err vs null).
5. ABSORPTION CASE NOT FIRED here. `(c0 ∧ c1) ∨ (c0 ∧ c1 ∧ c2)`: intersection
   `{c0,c1}`, one residual empty. Assert this rule does not factor it (leaves it
   for 2b). A differential is optional; the structural assertion that 2a did not
   collapse it is the point.

Confirm the existing 61 scalar tests still pass, and the phase-level
`test_canonicalize_eval_differential` still passes (it will exercise factoring
over random nested boolean exprs and is the cross-interaction soundness gate).

## Constraints (binding)

* Separate scalar engine; touch no relational eqsat code.
* No `as` conversions. No em-dashes, no clause-joining semicolons in comments.
* Doc comment states: the rewrite, the residual-error gate AND why it is on the
  residuals not the common factor (cite the CLU-137 narrowing), the
  non-empty-residual condition that keeps it distinct from absorption, and that
  it mirrors reduce's intersection path non-destructively.
* Reuse `is_and_or`, `switch_and_or`. Do not port `flatten_associative` or the
  subset/heuristic path.

## Done criteria

* `cargo check -p mz-transform --tests`, `cargo clippy -p mz-transform --tests`
  clean; `bin/fmt` applied. Consult the `mz-test` skill for the test invocation.
* All prior 61 scalar tests pass unchanged; new factoring tests pass; the
  phase-level differential passes. Report the new total.

## Commit
On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar undistribute factoring rule, residual-error gated (Phase 2)`.
End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report
Write your full report to `.superpowers/sdd/scalar-task-2a-report.md`. Return
only: status, commit sha, one-line test summary (new total), what you deferred
(subset factoring, nullability disjunct), concerns.
