# Task 2b: absorption rule (dropped-operand gated)

Phase 2 of the scalar equality-saturation canonicalizer. Adds the absorption
rewrite, the gated half of the CLU-137 split. Separate rule from 2a (factoring).
Depends on the 1h contract. Read the design note's CLU-137 + envelope section
first (`doc/developer/design/20260625_eqsat_scalar_expressions.md`).

## The rewrite (both duals)

```text
a ∨ (a ∧ c) → a     (outer Or absorbs the And that contains a)
a ∧ (a ∨ c) → a     (outer And absorbs the Or that contains a)
```

Generalized: in an outer And/Or, one operand whose inner-set is a SUPERSET of
another operand's inner-set is redundant and is dropped. Source: the absorption
special case folded into `MirScalarExpr::undistribute_and_or`
(`src/expr/src/scalar.rs:916`); the `reduce` docs describe Absorb-OR / Absorb-AND
(`scalar.rs:852` area). Non-destructive: union the absorbed form, do not mutate.

## Soundness gate (on the DROPPED extra conjuncts)

`a ∨ (a ∧ c) → a` is unsound when `a` can be null and `c` errors: original
`null ∨ (null ∧ err) = null ∨ err = err`, result `a = null`. The error came from
`c`, the operand being dropped (`a` is retained, so `a`'s own error still
surfaces in the result). So:

* GATE: drop the superset operand `Q` only when every operand in
  `inner-set(Q) \ inner-set(P)` (the EXTRA conjuncts being dropped, where `P` is
  the subset operand that stays) has `could_error == false`.

The retained operand `P` (and any error inside it) is NOT gated — it stays, so
its error is preserved. This mirrors 2a's principle: gate the operands whose
error behavior the rewrite would change (here the dropped extras), not the whole
expression. The dual `a ∧ (a ∨ c) → a` is symmetric (bad case `a=null`, `c`
errors via the inner Or; gate the dropped extras).

Exact-eval soundness with the gate (verify by hand both duals): with the dropped
extras error-free, for `a ∈ {true,false,null,error}` the absorbed form equals the
original. (`a=error`: retained, both sides error. `a=null`: `c` error-free so no
err-vs-null divergence. `a=true/false`: absorption law holds in Kleene.)

NOTE on the optional second disjunct: as with 2a, "the retained operand `P`
cannot be null" would also make it sound even if the extras can error, but it
needs a nullability analysis we lack. DEFER it; note in the report.

## The rule, step by step (outer Or; dual is symmetric via switch_and_or)

Add `absorb_and_or` to `rules.rs`, registered in `rules()`. Reuse `is_and_or`,
`switch_and_or`. For `node = CallVariadic { func: outer_func, exprs }` with
`is_and_or(outer_func)`:

1. `inner_func = outer_func.switch_and_or()`.
2. For each outer operand id, compute its inner-set of canonical class ids: if its
   class holds a `CallVariadic { func: inner_func, exprs }` node, that set; else a
   singleton `{find(operand)}` (the 1-arg view). (Same set computation as 2a;
   factor the shared helper if clean, but do not change 2a's behavior.)
3. Find an ordered pair of DISTINCT outer operands `(P, Q)` (by index) with
   `inner-set(P) ⊊ inner-set(Q)` (proper subset). If none, return `vec![]`.
4. GATE: if any id in `inner-set(Q) \ inner-set(P)` has `could_error == true`,
   this pair does not qualify; try other pairs. If no pair qualifies, `vec![]`.
5. For the first qualifying pair, build `Or` of all outer operands EXCEPT `Q`
   (drop `Q`), via `eg.add(CallVariadic { outer_func, kept_ids })` (read ids into
   an owned vec first, sort for determinism). If exactly one operand remains,
   return `vec![that id]` (let `and_or_single` not even be needed; returning the
   single id unions directly). Return `vec![result_id]`.

You may drop ONE qualifying `Q` per fire; saturation re-applies to remove further
redundant operands. Determinism: iterate operands by index, pick the first
qualifying `(P, Q)`; sort kept ids before `eg.add`.

Equality semantics: comparison of inner-sets is by canonical class id (stronger
than syntactic), so it absorbs across operands proven equal by earlier rules.

## Do not double-count with 2a / Phase 1

2a (factoring) only fires with non-empty residuals, so it never performs
absorption. Phase 1 has no absorption rule. So 2b is the sole absorption path,
and it carries the gate. Confirm you are not re-implementing factoring.

## Tests (rules.rs test module, existing style)

1. SOUNDNESS over the cube: `assert_eval_equiv(c0 ∨ (c0 ∧ c1), 2)` and the dual
   `assert_eval_equiv(c0 ∧ (c0 ∨ c1), 2)` (c1 error-free column, so it fires).
2. FIRING: `canonicalize(c0 ∨ (c0 ∧ c1))` extracts `c0` (cheapest); dual extracts
   `c0`. Proves the rule fired.
3. GATE ALLOWS retained-operand error: `g ∨ (g ∧ c1)` with `g = (1/c0 = 5)`
   (errors at c0==0, the RETAINED operand P=g), c1 error-free. Assert it absorbs
   to `g`, and differential `eval(input)` vs `eval(canonicalize(input))` over
   c0 ∈ {0, ...} (g errors) and c1 over {t,f,null}: equal, because g is retained
   so its error still surfaces. Proves absorption fires when the RETAINED operand
   errors.
4. GATE BLOCKS dropped-extra error (CLU-137-class soundness): `c0 ∨ (c0 ∧ r)`
   with `r = (1/c1 = 5)` (errors at c1==0, the DROPPED extra). Assert the rule
   does NOT absorb to `c0` (output is not `c0`; the term stays), and a
   differential over c0 ∈ {t,f,null} and c1==0 holds exact-eval equality (which it
   does because blocking keeps the original). Witness: at c0=null, r=err the
   original is `err` while the unsound absorbed `c0` would be `null` — so without
   the gate the differential would fail. Make this explicit (assert input evals to
   Err at that row).

Confirm all 66 prior scalar tests pass and `test_canonicalize_eval_differential`
still passes (it now exercises factoring AND absorption interaction).

## Constraints (binding)

* Separate scalar engine; no relational code. No `as`. No em-dashes /
  clause-joining semicolons in comments.
* Doc comment: the rewrite, the dropped-extra gate and why it is on the dropped
  extras not the retained operand or whole expr, and that it is the gated
  counterpart to 2a's factoring (the CLU-137 split).
* Reuse `is_and_or`, `switch_and_or`, and the 2a inner-set computation if you can
  share it cleanly without changing 2a behavior.

## Done criteria
`cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt` applied (consult
the `mz-test` skill for the test command). All 66 prior scalar tests pass; new
absorption tests pass; differential passes. Report the new total.

## Commit
On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar absorption rule, dropped-operand gated (Phase 2)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report
Write your full report to `.superpowers/sdd/scalar-task-2b-report.md`. Return
only: status, commit sha, one-line test summary (new total), what you deferred,
concerns.
