# Task 1c report: scalar boolean-algebra and NOT rewrite rules

## Status

DONE.

## Commit

`3b6777d33f8842ac7896e03712df8b1c9dca9a4f` on branch
`claude/mir-equality-optimizer-sodbej`.
Subject: `eqsat: scalar boolean-algebra and NOT rules (Phase 1)`.

## What was implemented

All seven rules from the brief were added to
`src/transform/src/eqsat/scalar/rules.rs`, registered in `rules()`, with no
infrastructure changes and no change to the 1b rule contract
(`fn(eg, node) -> Vec<SNode>`).

1. `and_or_dedup`: drops duplicate operands of `And`/`Or` by canonical id via
   `eg.find`, preserving first-occurrence order. This is the order-independent
   form of reduce's sort-then-`dedup`.
2. `and_or_single`: a one-arg `And`/`Or` unions with its sole child class by
   returning `eg.nodes(exprs[0])`. The apply step re-adds those nodes, hash-cons
   returns the existing class, and the union merges the two classes.
3. `and_or_empty`: a zero-arg `And`/`Or` produces the unit literal via
   `func.unit_of_and_or()` destructured into `SNode::Literal`.
4. `and_or_short_circuit`: `And` with any `lit_bool == Some(false)` produces
   `false`; `Or` with any `Some(true)` produces `true` (the connective's zero).
5. `and_or_drop_unit`: `And` drops `true` operands, `Or` drops `false` operands.
   Fires only when at least one unit was removed; the resulting collapse to one
   or zero operands is left to rules 2/3 on a later iteration.
6. `not_not`: `Not(Not(x))` unions with the grandchild class by returning its
   nodes.
7. `not_binary_negate`: `Not(a bf b)` produces `CallBinary { negate(bf), e1, e2 }`
   over the existing operand ids when `bf.negate()` is `Some`.

A `lit_bool(eg, id) -> Option<bool>` helper returns `Some(true)`/`Some(false)`
only for non-null, non-error boolean literals, so null and error operands never
match the true/false arms, matching reduce, which compares against the concrete
`literal_true()`/`literal_false()` values.

De Morgan was intentionally NOT implemented, per the brief. It is the only
boolean rewrite that must construct nested NEW nodes (`OR(Not a, Not b)`), which
the read-only rule contract cannot express. No contract change was needed for
the seven implemented rules, so no rule-8 apply-step extension was made.

## Tests (the soundness gate)

`src/transform/src/eqsat/scalar/rules.rs` carries one property test per rule. The
gate is `assert_eval_equiv`, which differentials `canonicalize(&expr)` against
`expr` by evaluating both over the full `{true, false, null}` cube
(`bool_cube(n)` enumerates all `3^n` assignments) and comparing owned results,
including `Err` and `Null`. The short-circuit test additionally uses an all-error
`1/0`-derived operand inside the `And` to confirm error short-circuit semantics
are preserved.

Each property test also carries a structural `assert_eq!` on the canonical output
(e.g. `AND(c0, c0, c1)` dedups to `AND(c0, c1)`, `Not(c0 = c1)` becomes
`c0 != c1`). These give the tests teeth: the eval-differential alone passes
trivially when a rule does not fire, because the unrewritten term is
observationally equal to itself. Disabling the rule list down to `const_fold`
confirmed that the six column-operand rule tests fail without their rule, then
pass once the rules are restored.

One-line summary: `cargo test -p mz-transform --lib eqsat::scalar` reports
`37 passed; 0 failed` (7 new rule property tests plus all prior Phase 0/1a/1b
tests).

Done criteria: `cargo check -p mz-transform --tests` clean,
`cargo clippy -p mz-transform --tests` clean, `cargo fmt -p mz-transform`
applied.

## Concerns

* `test_and_or_empty` and `test_and_or_short_circuit_with_error` pass even with
  their dedicated rules disabled, because `const_fold` already folds a
  fully-literal `AND()`/`OR()` or an all-error AND to the same result. The
  dedicated rules still mirror reduce exactly and are exercised by the
  column-operand tests, so this is redundancy rather than a coverage gap. I left
  the dedicated rules in place because the brief requires all seven and reduce
  has them; const_fold is a second, equally-sound path for the constant cases.

* No other concerns. The eval-differential confirms 3-valued-logic and AND/OR
  error short-circuit semantics are preserved for every rule.
