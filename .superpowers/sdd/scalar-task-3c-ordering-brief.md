# Task 3c-ordering: canonical operand ordering at extraction

Phase 3 of the scalar equality-saturation canonicalizer. Make extraction emit
commutative AND/OR operands in the SAME canonical order `MirScalarExpr::reduce`
uses, so optimizer EXPLAIN goldens differ from `reduce` only where eqsat genuinely
changes the plan, not from operand-order noise. This is the plan's deferred
"canonical operand ordering (sort commutative operands at extraction, NOT a
saturation rule)".

## Why at extraction, not a rule

A saturation rule that unions permutations explodes the e-graph (n! orderings).
Instead, sort at RAISE time (reconstruction), exactly once, on the final extracted
operands. `reduce` sorts And/Or operands via `exprs.sort()` inside
`reduce_and_canonicalize_and_or` (`src/expr/src/scalar.rs:805`), using
`MirScalarExpr`'s derived `Ord`. Mirror that: sort the reconstructed And/Or
operands by `MirScalarExpr`'s `Ord`.

## Scope: And/Or only

`reduce` sorts operands only for `And`/`Or` (the sort lives in the And/Or
canonicalization arm). Other variadics (Coalesce, etc.) are order-significant and
are NOT sorted. So sort ONLY when the variadic func is And or Or (reuse the
`is_and_or` predicate from rules.rs, or an equivalent check). Do NOT sort other
variadics, unary, binary, or If.

## Where

`src/transform/src/eqsat/scalar/raise.rs`, the `reconstruct` function's
`SNode::CallVariadic` arm (around raise.rs:154). After building the
`Vec<MirScalarExpr>` operands (via `build` on each child), if the func is And/Or,
`sort()` the operand vec before constructing the `MirScalarExpr::CallVariadic`.
Keep the non-And/Or path unchanged (preserve operand order).

## Soundness

Sorting And/Or operands is exact-eval safe: And/Or are an order-independent join
(value is the same regardless of operand order; the surfaced error is the
order-independent `max` over operand errors; the short-circuit value does not
depend on order). `reduce` reorders freely for the same reason. So this changes
only presentation, not semantics. The phase-level differential test
(`test_canonicalize_eval_differential`) must still pass (it compares eval, which
is order-invariant for And/Or).

NOTE: this is a deterministic, total sort by `Ord`, so it also removes the
HashSet-iteration-order nondeterminism in the extracted And/Or operand order
(a strengthening over the prior Minor notes).

## Tests

1. ORDERING: `canonicalize(And(c1, c0))` (or with a literal and a column in
   non-sorted order) extracts operands in `MirScalarExpr::Ord` order, i.e. the
   same order `reduce` would produce. Assert the exact sorted operand vector.
   Construct an input whose natural lowering order is NOT already sorted so the
   sort is observable. Dual for Or.
2. STABILITY/DETERMINISM: the extracted order is deterministic across runs (a
   plain assertion on the sorted result suffices; the sort makes it total).
3. SOUNDNESS UNCHANGED: `assert_eval_equiv` on a sorted-output And/Or, and confirm
   the existing differential test still passes.
4. NON-AND/OR UNCHANGED: a Coalesce (or other order-significant variadic) keeps
   its operand order (do NOT sort it). If a convenient order-significant variadic
   is awkward to build, at minimum assert that a non-And/Or path is untouched
   (e.g. an If's branch order, or a binary's operand order).

Confirm all prior scalar tests pass. Some EXISTING scalar rule tests assert exact
And/Or operand orders (e.g. factoring/De Morgan firing tests asserting
`OR(c0, c1)` or `AND(c0, c1)`); if any now come out in a different (sorted) order,
update ONLY the asserted order to the sorted one (this is the same canonical order,
not a behavior change) and note which tests you touched. Do not change their eval
assertions.

## Constraints (binding)

* Separate scalar engine; no relational code. No `as`. No em-dashes /
  clause-joining semicolons in comments.
* Sort And/Or ONLY; mirror reduce. Doc-comment the sort point: why at extraction,
  why And/Or only, and that it matches reduce's `exprs.sort()` for golden parity
  and is exact-eval safe (order-independent join).
* Do not add a saturation rule for ordering.

## Done criteria

`cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt` applied (consult
the `mz-test` skill for the test command). All scalar tests pass (with any
operand-order assertions updated to sorted order); differential passes. Report
the new total and which existing tests you re-ordered.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: canonical AND/OR operand ordering at extraction (Phase 3)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-3c-ordering-report.md`.
Return only: status, commit sha, one-line test summary (new total + which existing
tests were re-ordered), concerns.
