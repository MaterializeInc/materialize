# Miscompile fix report

## Background

The eqsat MIR optimizer miscompiled builtin views, crashing rendered dataflows with
"Non-positive multiplicity" errors and a connection-drop in sqllogictest.
Three distinct unsoundness bugs were found and fixed.

## Bug 1: Circular join-condition rewriting (original bug)

**Root cause.** `rewrite_escalars` applied a join's *own* output equivalences back to
rewrite its own join conditions.
The Equivalences analysis for `Join[#a = #b](...)` derives `#b → #a` from the condition.
Feeding that back into the conditions rewrites `[#a, #b]` to `[#a, #a]`, silently
removing the equijoin constraint.
The resulting cross-product leaked extra rows into the outer-join
`Threshold/Union/Negate` pattern and produced negative multiplicities.

**Fix.** `rewrite_escalars` returns `None` for `ENode::Join { .. } | ENode::WcoJoin { .. }`.
Join conditions are never rewritten by the saturation-time scalar canonicalization pass.
Canonicalization of join equivalences is a typed-phase responsibility.

**Files changed.**
- `src/transform/src/eqsat/egraph.rs` — added `ENode::Join | ENode::WcoJoin => None` arm.
- `src/transform/tests/test_transforms/eqsat.spec` — case (h) expected output updated
  to keep `Join on=(#0 = #2)` instead of the incorrectly collapsed `Join on=(#0 = #0)`.

## Bug 2: `factor_negate_join` unsound for non-linear aggregates

**Root cause.** The rule `join(negate(a), rest) => negate(join(a, rest))` merged
`join(negate(a), rest)` and `negate(join(a, rest))` into the same e-class.
When a `Reduce` with a non-linear aggregate (MAX, MIN, ANY, ALL) sat above the join's
e-class, the extractor could pick `negate(join(a, rest))` as the Reduce input, producing
`Reduce_MAX(negate(join(a, rest)))`.
This is semantically wrong: `reduce(r) ≠ negate(reduce(negate(r)))` for non-linear
aggregates.

**Fix.** The rule `factor_negate_join` was removed from `relational.rewrite`.
The opposite direction `distribute_negate_join` — `negate(join(a, rest)) =>
join(negate(a), rest)` — was also removed: e-graph merging is bidirectional, so once
either rule fires both forms are in the same class, exposing the same extractor bug.

**Files changed.**
- `src/transform/src/eqsat/rules/relational.rewrite` — both rules removed; explanatory
  comment added.
- `src/transform/lean/MirRewrite/Generated.lean` — regenerated via `gen-lean`.
- `src/transform/tests/roundtrip.rs` — `negate_join_unlocks_cancellation` renamed to
  `negate_join_no_longer_cancels`; updated to not assert `is_empty_constant` since the
  cancellation no longer fires.

## Bug 3: Circular filter-predicate rewriting

**Root cause.** `rewrite_escalars` for `ENode::Filter { input, predicates }` used the
Filter's *own* output e-class reducer to rewrite the predicates.
The Filter's Equivalences analysis derives a class `[pred_1, ..., pred_n, true]` from
the predicates; after `minimize`/`remap` the reducer maps each predicate's sub-expressions
to canonical representatives derived from those predicates.
Applying that reducer back to the predicates is circular: for example,
`Filter[#2 = CurrentUser()] r` would have `CurrentUser() → #2` in its reducer.
Rewriting the predicate yields `#2 = #2` (trivially true), which caused `drop_true_filter`
to eliminate the filter entirely — including security-relevant filters such as
`WHERE name = current_user`.

**Fix.** `rewrite_escalars` now takes an additional `filter_input_reducer` parameter.
For `ENode::Filter`, Phase 2a looks up the *input's* e-class reducer from `analyses.eq`
and passes it as `filter_input_reducer`.
The Filter predicate rewrite uses only this input reducer; the Filter's own reducer
(which contains the circular facts) is ignored for predicates.

For `ENode::Map` the class-level reducer remains correct: the Map's output equivalences
include new-column facts at indices `>= input_arity`, but the column-range guard in
`apply` rejects any rewrite that would introduce such out-of-range references into the
scalars themselves, preventing the analogous circular rewrite.

**Files changed.**
- `src/transform/src/eqsat/egraph.rs` — `rewrite_escalars` signature gains
  `filter_input_reducer: Option<&BTreeMap<...>>`; Phase 2a computes the input's reducer
  for each Filter node; Filter arm uses `filter_input_reducer?`; all test call sites
  updated.
- `src/transform/tests/test_transforms/eqsat.spec` — case (f) expected output updated;
  comment expanded to explain why the filter predicate now stays as `#0 = #1` instead of
  being rewritten to `#0 = #0`.

## Verification

- `cargo test -p mz-transform`: all 55 + 22 + ... tests pass (previously 2 tests were
  pre-existing failures due to bug 3; they now pass).
- `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`: PASS 206/206,
  zero "Non-positive multiplicity" occurrences.
- `bin/lint` (Rust-specific checks): rustfmt and check-cargo.sh pass.
