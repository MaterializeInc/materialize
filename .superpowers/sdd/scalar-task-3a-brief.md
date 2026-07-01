# Task 3a: associativity-flattening rule (flat conjunction)

Phase 3 prerequisite for the scalar equality-saturation canonicalizer. Adds an
AND/OR associativity-flattening rewrite so the canonicalizer's output is a FLAT
conjunction, matching what `MirScalarExpr::reduce` guarantees. This is required
because downstream (`canonicalize_predicates`, `src/expr/src/relation/
canonicalize.rs:233`) explicitly "relies on the `reduce` above having flattened
nested ANDs". Depends on the 1h contract (build nested nodes via `eg.add`, return
`Vec<Id>`).

## The rewrite

```text
And(a, And(b, c)) -> And(a, b, c)
Or(a, Or(b, c))   -> Or(a, b, c)
```

Splice a nested same-connective variadic's operands into the parent. Source of
truth: `MirScalarExpr::flatten_associative` (`src/expr/src/scalar.rs:758`). It
flattens one level per application for any `func.is_associative()` variadic;
saturation re-applies for deeper nesting.

## Soundness: UNCONDITIONAL, no gate

And/Or are a join semilattice over the total order `{False, Null, Error(by max),
True}` for And (dually for Or): the result value is order-independent (any false
forces false; otherwise the max error; otherwise null; otherwise true). So
`And(a, And(b,c))` and `And(a,b,c)` evaluate identically for ALL inputs including
errors and short-circuit (the short-circuit value and the max-error reduction are
both order-independent, and an unevaluated nested operand under a false
short-circuit contributes nothing on either side). No `could_error` gate, no
nullability gate. State this in the doc comment (associativity is exact-eval
sound, mirrors reduce's flatten_associative).

## The rule

Add `flatten_assoc` to `rules.rs`, registered in `rules()`. For
`node = CallVariadic { func, exprs }`:

1. Return `vec![]` unless `func.is_associative()` (matches reduce; covers And/Or
   and any other associative variadic). Confirm `is_associative` exists on
   `VariadicFunc` (it does, used by `flatten_associative`).
2. Build a new operand id list by flat-mapping over `exprs`: for each operand id,
   read its class nodes (`eg.nodes(find(id))`); if the class contains a
   `CallVariadic { func: same_func, exprs: inner }` node (same `func` as the
   parent), splice in `inner` (its child ids); otherwise keep the operand id.
   (Read everything into owned values before any `eg.add`, per 1h borrow
   discipline. If a class holds several same-func variadic nodes, the first is
   fine, they are equal; keep it deterministic.)
3. If nothing was spliced (the new list equals the original operands), return
   `vec![]` (no-op, do not union a copy of yourself).
4. Otherwise `eg.add(CallVariadic { func, exprs: new_ids })` and return
   `vec![that]`.

Determinism: preserve operand order while flat-mapping (do NOT sort here, unlike
the set-based rules; flattening is order-preserving and other rules handle
dedup/ordering). Hashcons keys on the operand vector, so a stable order keeps the
node stable.

Termination: the flattened node has the same leaves but fewer interior variadic
nodes, so it is structurally smaller and hashconsed; re-firing on it splices
nothing (no same-func child) and returns `vec![]`. Confirm saturation stays
within bounds.

Extraction: the flattened form is cheaper (one fewer variadic node per spliced
level), so min-cost extraction prefers it. That is what delivers the flat
conjunction.

## Tests (rules.rs test module, existing style)

1. FIRING + soundness: `canonicalize(And(c0, And(c1, c2)))` extracts a single flat
   `And(c0, c1, c2)` (assert flat: outer And has three column operands, none of
   them a nested And). Pair with `assert_eval_equiv` over the cube. Dual for Or.
2. DEEP nesting via saturation: `And(c0, And(c1, And(c2, c3)))` flattens fully to
   `And(c0,c1,c2,c3)`. Proves saturation re-applies.
3. ERROR-operand preservation: a nested form with a non-literal erroring operand,
   e.g. `And(c0, And((1/c1 = 5), c2))`, flattened to `And(c0, (1/c1=5), c2)`;
   differential `eval(input)` vs `eval(canonicalize(input))` over rows incl.
   c1==0 (error) and c0/c2 over {t,f,null}. Proves flattening preserves exact eval
   including the error. (It must, associativity is unconditional.)
4. NO-OP: an already-flat `And(c0, c1)` is unchanged (the rule does not fire / the
   result is unchanged), and a mixed `And(c0, Or(c1, c2))` does NOT splice the Or
   into the And (different connective).

Confirm all prior scalar tests pass and `test_canonicalize_eval_differential`
still passes. NOTE: the existing differential generator builds nested And/Or, so
this rule now fires inside it; the differential remains the cross-interaction
soundness gate.

## Constraints (binding)

* Separate scalar engine; no relational code. No `as`. No em-dashes /
  clause-joining semicolons in comments.
* Doc comment: the rewrite, unconditional-soundness rationale (associativity is
  order-independent so exact-eval sound, no gate), mirrors reduce's
  flatten_associative, and that it delivers the flat-conjunction form downstream
  relies on.
* Reuse existing helpers where natural; do not change other rules' behavior.

## Done criteria
`cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt` applied (consult
the `mz-test` skill for the test command). All prior scalar tests pass; new
flattening tests pass; differential passes. Report the new total.

## Commit
On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar AND/OR associativity flattening rule (Phase 3)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report
Write your full report to `.superpowers/sdd/scalar-task-3a-report.md`. Return
only: status, commit sha, one-line test summary (new total), concerns.
