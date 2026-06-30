# Task 2a review: undistribute factoring rule (residual-error gated)

Base f4bd89bd7e .. Head bade736500. Read-only review.

## Spec Compliance / completeness

- ✅ Rule `factor_and_or` added and registered once in `rules()` (`rules.rs:42`),
  between `and_or_drop_unit` and `not_not`. Diff stat is `408 insertions, 0
  deletions`, so no existing rule or test was touched/weakened.
- ✅ Both duals handled via `is_and_or` + `switch_and_or` (`rules.rs:334-338`).
- ✅ FULL intersection across all branches in one step (`rules.rs:140-143`), not
  incremental; empty intersection → `vec![]` (`rules.rs:144-148`). Mirrors
  reduce's `!intersection.is_empty()` path (`scalar.rs:916`) non-destructively.
- ✅ Non-empty-residual condition: any empty residual → `vec![]`
  (`rules.rs:158-163`), before the gate and before any build.
- ✅ Residual-error gate on residuals only; factor exempt (`rules.rs:169-173`).
- ✅ Inner-operand SET computation via `eg.nodes(canon)` scan for the
  `inner_func` node, else singleton; canonical/sorted/deduped ids
  (`rules.rs:120-136`). Mirrors `not_not`'s node scan.
- ✅ Borrow discipline: all sets read into owned values before `eg.add`
  (`rules.rs:175-203`).
- ✅ Doc comment states the rewrite, the residual-error gate + why it is on the
  residuals not the factor (the CLU-137 narrowing of reduce's
  `self.could_error()`), the non-empty-residual condition vs absorption, and the
  non-destructive mirroring (`rules.rs:282-310`).
- ✅ No `as`, no em-dashes, no clause-joining semicolons in comments; subset
  factoring + nullability disjunct deferred and noted in the report.
- ✅ All 5 brief tests present; report claims 66 pass incl.
  `test_canonicalize_eval_differential`.
- ⚠️ Determinism caveat: `classes` is `HashMap<Id, HashSet<SNode>>`
  (`egraph.rs:74`), so `nodes()` iteration order is non-deterministic. "First
  `inner_func` node" can vary run-to-run when a class holds several. Sound (those
  nodes are eval-equal), consistent with `not_not`. See Minor.

## Strengths

- The gate is exactly right: it gates every residual operand and deliberately
  exempts the factor. I worked the full value cube for both duals (below) and it
  is exact-eval sound for all factor values including null and error.
- The non-empty-residual guard is the correct and only structural separation
  from absorption, and it runs before the gate, so the absorption case never
  reaches build.
- The set-based construction is sound because dedup/grouping of And/Or operands
  is idempotent under exact eval, and class members are eval-equal, so picking
  any `inner_func` representative is safe.
- Tests are well-targeted: the gate-block test carries a real witness row, and
  the erroring-common-factor test genuinely exercises the CLU-137 property.

## Issues

### Critical
None.

### Important
None.

### Minor
- `rules.rs:198-202`: `factored_exprs` is `intersection` (sorted) with
  `residual_combination` appended last and never re-sorted. This is deterministic
  and hashcons-stable (same inputs → same vector each fire), so termination/dedup
  are fine, but the produced And/Or node is not canonically operand-sorted,
  unlike `branch_ids` which is explicitly sorted (`rules.rs:193`). Cosmetic only.
- `rules.rs:124` / `egraph.rs:312`: `nodes()` returns from a `HashSet`, so "first
  `inner_func` node" is order-nondeterministic when a class holds multiple.
  Soundness is unaffected (members are eval-equal) and it mirrors `not_not`, but
  a one-line comment noting that determinism rests on member-equivalence (not
  iteration order) would help future readers. Not a regression.

## Soundness verdict

**Gate correct & on residuals only? Yes.** The gate (`rules.rs:169-173`) checks
`eg.analysis(id).could_error` for every operand of every residual and exempts the
intersection. `could_error` is a safe over-approximation that converges before
rules run and only rises false→true (`egraph.rs:238-247`, `analysis.rs:32`), so
`could_error == false` is a trustworthy "cannot error". With residuals confined
to {true,false,null}, I verified exact-eval equality of `Or_i(F ∧ R_i)` vs
`F ∧ Or_i(R_i)` (undistribute-OR) and the dual `And_i(F ∨ R_i)` vs
`F ∨ And_i(R_i)` (undistribute-AND), against the real And/Or eval
(`variadic.rs:80` / `:1146`: short-circuit on the connective's zero, otherwise
error wins via `max`, null only when no error), for F ∈ {true, false, null,
error}:

- **F = error (CLU-137 property).** OR-dual: each `F ∧ R_i` is `false` if
  `R_i = false` else `error`; the outer Or yields `false` iff all `R_i = false`,
  else `error`. Factored `error ∧ (Or R_i)` yields `false` iff `Or R_i = false`
  (all `R_i false`), else `error`. Identical. An erroring factor is preserved,
  never masked. AND-dual symmetric: both reduce to `true` iff all `R_i = true`,
  else `error`.
- **F = null + a residual error worked case.** OR-dual, F=null: `null ∧ R_i` is
  `false` if `R_i=false` else `null`; outer Or = `null` iff some `R_i ≠ false`,
  else `false`. Factored `null ∧ (Or R_i)` = `null` iff `Or R_i ∈ {true,null}`
  (some `R_i ≠ false`), else `false`. Identical. The design note's dangerous row
  (factor=null, one residual true, another residual error) is excluded because
  the gate forbids an erroring residual. AND-dual, F=null (masking via
  short-circuit): Original = `null` iff some `R_i ≠ true`, else `true`; Factored
  matches via `And R_i`; a residual that masks (false) plus another that errors
  is excluded by the gate.
- **F = true / false** collapse to `Or_i R_i` / the connective's zero
  respectively on both sides. Identical.

**Non-empty-residual separation from absorption sound? Yes.** Empty residuals are
rejected (`rules.rs:158-163`), so the ungated absorption path the brief warns
about (empty inner call → unit → drop-to-factor) is unreachable. I also checked
the subtler non-empty path where a residual is the literal `true`: e.g.
`Or(And(a,true), And(a,c))` factors to `And(a, Or(true,c))`, which Phase 1
collapses to `a` (effectively an absorption). This is still sound, because the
residual `c` is gated error-free, which is exactly the precondition a sound
absorption needs. Every reachable collapse-to-absorption is over gated,
error-free residuals, so no ungated absorption can be produced.

## Assessment

**Task quality:** Approved

**Reasoning:** The factoring rule is exact-eval sound for both duals across the
full value cube including factor=null and factor=error, the residual-error gate
is on the correct operands (residuals, not the factor), and the
non-empty-residual guard cleanly and soundly keeps factoring distinct from
absorption so the CLU-137 class cannot reappear; only two cosmetic minors remain.
