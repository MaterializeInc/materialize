# Phase 3c: duplicate IS NOT NULL predicate (join_index.slt) + literal_constraints reorder

## Status
DONE. Commit `efe2a4c0a3`.

## Root cause (Finding 1, the duplicate predicate)
Query: `select foo.b, bar.b from foo, bar where nullif(foo.a, 0) = -bar.a`.

The duplicate is NOT produced inside `canonicalize_predicates`, and is NOT
flag-toggled there. Bisecting the transform pipeline (instrumented
`Transform::transform` to count duplicate filter predicates before/after each
pass) showed the sequence on BOTH flag states:

- `PredicatePushdown` introduces two IS-NOT-NULL forms on the join key:
  - `NOT(IsNull(#2))`            (IS NOT NULL bar.a)
  - `NOT(IsNull(NegInt32(#2)))`  (IS NOT NULL of `-bar.a`)
  These are structurally distinct predicates (`!=`), not a single `And(X,X)`.
- flag-OFF: `Fusion`/`canonicalize_predicates` step 1 is `reduce`, whose
  `decompose_is_null` rewrites `IsNull(f(x)) -> IsNull(x)` for null-propagating
  unary `f` (`src/expr/src/scalar.rs:708`). Both forms converge to
  `NOT(IsNull(#2))`, then step 6 sort+dedup collapses them. Single predicate.
- flag-ON: step 1 is the eqsat canonicalizer, which deliberately does NOT do
  that rewrite. `NegInt32` can overflow, so `IsNull(NegInt32(MIN))` is an error
  while `IsNull(MIN)` is `false`. The canonicalizer holds an exact-eval contract
  (guarded by `test_canonicalize_eval_differential`) and is strictly more
  error-conservative than reduce. `decompose_is_null` is explicitly deferred
  (rules.rs:1132). So the two forms stay distinct and step 6 dedup cannot match
  them.
- Late in the pipeline `ReduceScalars` runs plain `reduce` on each predicate,
  which finally converges `NOT(IsNull(NegInt32(#2))) -> NOT(IsNull(#2))`, making
  the two identical, but `ReduceScalars` does not dedup its Filter predicate
  list (its adjacent Join branch DOES dedup equivalences). No
  `canonicalize_predicates` runs after it. The duplicate survives to the plan.

So the eqsat-rule fix is correctly off the table: adding `IsNull(-x)->IsNull(x)`
to eqsat would drop the overflow error and fail the differential test.

## Fix
`src/transform/src/canonicalization.rs`, `ReduceScalars` Filter branch: after
reducing predicates, drop duplicates with an order-preserving dedup
(`BTreeSet` + `retain`). Order preserved (predicate order is displayed, and no
`canonicalize_predicates` runs after every `ReduceScalars`); no sort (sorting
would reorder flag-off plans). Removing a redundant `P AND P = P` conjunct is
always sound.

Flag-off unaffected: on flag-off, reduce-based `canonicalize_predicates` already
converges+dedups these forms before `ReduceScalars`, so the new dedup is a
no-op. Confirmed by the flag-off datadriven `test_transforms` suite passing
unchanged.

## Finding 2 (literal_constraints.slt): benign
The two top-level AND conjuncts are operand-order swapped only
(`(like OR a>5)` and `(a<3 OR a<7 OR a>0)`), same conjuncts, nothing
added/removed/changed. The eqsat fast path also renders them as one `And`
predicate (outer parens) vs two list entries. Pure operand-order/grouping from
the eqsat raise-time And-operand sort. Benign; regenerated.

## Goldens regenerated
- join_index.slt: NO diff after the fix. Flag-on plan now matches the committed
  golden exactly (single `(#2{a}) IS NOT NULL`); the dup is gone.
- literal_constraints.slt: benign conjunct reorder (1 line).

## Verification
- join_index.slt 72/72, literal_constraints.slt 132/132 pass flag-on.
- All 288 mz-transform tests pass, incl `test_canonicalize_eval_differential`
  and the flag-off `test_transforms` datadriven suite.
- `cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt` clean.

## Concerns
None material. The dedup is order-preserving and sound; flag-off is a no-op.
The broader latent gap (eqsat does not implement `decompose_is_null`) remains
deferred by design; this fix makes the downstream plan converge regardless.
