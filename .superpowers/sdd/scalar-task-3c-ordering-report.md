# Task 3c-ordering Report

## Status: DONE

## Commit

`af34355306` — `eqsat: canonical AND/OR operand ordering at extraction (Phase 3)`

## What was done

Modified `raise.rs` `reconstruct`'s `SNode::CallVariadic` arm to sort operands
by `MirScalarExpr::Ord` when the func is `And` or `Or`, mirroring
`reduce_and_canonicalize_and_or`'s `exprs.sort()`. All other nodes (other
variadics, unary, binary, If) are left in their original order. Added
`VariadicFunc` import to `raise.rs`.

## Test summary

**282 tests total, all pass (1 skipped, pre-existing).**

Existing tests with operand-order assertions updated to sorted order:

1. `rules.rs::test_not_demorgan_error_preservation` — assertion changed from
   `OR(d, c1)` to `OR(c1, d)` (Column(1) sorts before CallBinary by enum
   discriminant). Comment updated to explain why.

2. `scalar.rs::round_trip_nested_mix` — input And operands reordered from
   `[NOT(c0), c1]` to `[c1, NOT(c0)]` so the round-trip is still identity
   (assert_round_trip requires raise(lower(e)) == e, which only holds when e
   is already in canonical sorted order).

New tests added (all pass):

- `test_ordering_and` — AND(c1, c0) extracts as AND(c0, c1); sort is observable.
- `test_ordering_or` — OR(c1, c0) extracts as OR(c0, c1); dual.
- `test_ordering_deterministic` — two canonicalize calls on same input agree.
- `test_ordering_soundness` — assert_eval_equiv on AND(c1, c0) and OR(c1, c0).
- `test_ordering_non_and_or_unchanged` — MakeTimestamp preserves operand order.

Differential test (`test_canonicalize_eval_differential`) passes unchanged.

## Concerns

None. The change is minimal and mechanical.

## Report path

`.superpowers/sdd/scalar-task-3c-ordering-report.md`
