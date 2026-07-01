# Phase 3c fix: empty Filter left around joins when the scalar canonicalizer is on

## Status

FIXED and verified. Single-file change in `src/transform/src/canonicalization.rs`
(`ReduceScalars`), plus a focused regression unit test.

## Root cause

The regression is NOT in the flag wiring and NOT in differing predicate content.
It is a capability gap in the eqsat scalar canonicalizer that strands an empty
`Filter` shell at a downstream pass that does not remove empty filters.

Chain (reproduced with `EXPLAIN OPTIMIZER TRACE` on `SELECT * FROM t, u WHERE
t.b = u.c` with `enable_eqsat_scalar_canonicalize = true`):

1. The regression only appears once the `u` side is read from an INDEX. With an
   index import, the `u`-side non-null requirement cannot stay below the shared
   index, so during global optimization a top-level `Filter (#1{b}) IS NOT NULL`
   is lifted over the differential `Join` (`#1` is `b`, equated to `c` via the
   join equivalence). `EXPLAIN LOCALLY OPTIMIZED PLAN` is clean; the single-view
   transform harness never reproduces it. It is a global/dataflow-stage effect.

2. `MirScalarExpr::reduce` folds `(#1) IS NOT NULL` to `true` using column
   nullability (the join output type marks `#1` non-nullable because of the
   `t`-side filter). The eqsat scalar canonicalizer (`eqsat::scalar::rules()`)
   has NO isnull/nullability fold, so it leaves `NOT(isnull(#1))` unchanged.

3. Flag OFF: physical `canonicalize_mfp` -> `fusion::filter::Filter::action` ->
   `canonicalize_predicates` reduces the predicate to `true`, empties the list,
   and `Filter::action` removes the now-empty `Filter`. Clean.

4. Flag ON: the same `canonicalize_predicates` runs the eqsat canonicalizer in
   step 1, which does NOT fold the predicate, so the `Filter` survives
   `canonicalize_mfp` non-empty.

5. The predicate then reaches `ReduceScalars` (`reduce_scalars`), whose `Filter`
   arm runs `predicate.reduce(input_type)` using the whole-tree
   `ReprRelationType` analysis. That folds it to `true` and drops it via
   `retain(|p| !p.is_literal_true())`, leaving an EMPTY predicate list. But
   `ReduceScalars` did not elide the emptied `Filter`, so an empty `Filter`
   shell is stranded and persists to the final plan.

First trace segment showing the empty Filter:
`optimize/global/physical/fold_constants_fixpoint/0000/reduce_scalars`.

## Fix

`ReduceScalars`, `Filter` arm: after dropping `true` predicates, if the
predicate list is now empty, replace the `Filter` with its input (and continue
descending into that input). This is option (a) from the brief: elide the empty
`Filter` at the offending path. It is not flag-gated, fixes the symptom at its
source, touches no flag wiring (so CLU-137 is untouched), and removes only a
runtime no-op (an empty `Filter`).

The fix does not change flag-off behavior: with the flag off the redundant
predicate is already folded and the empty `Filter` removed by
`canonicalize_mfp` before `ReduceScalars` runs, so the new branch is never
reached for those plans.

## Verification

- Regression gone (flag ON): rewrote the whole
  `test/sqllogictest/explain/optimized_plan_as_text.slt` with
  `enable_eqsat_scalar_canonicalize = true`; the only diff vs the committed
  golden is one injected blank line. All 4 equi-join EXPLAINs now match the
  committed flag-off plan exactly (no empty Filter). `pushdown.slt` flag-on is
  also identical to committed.
- Flag-off byte-identical (hard bar): committed `optimized_plan_as_text.slt`
  passes 119/119 unchanged; all `test/sqllogictest/explain/*.slt` pass 838/838
  unchanged; `mz-transform` datadriven `test_runner` and `test_transforms` pass
  with no REWRITE; `mz-transform` 283/283 and `mz-expr` 74/74 pass.
- CLU-137 still works:
  `eqsat::scalar::rules::tests::test_clu137_temporal_factors_to_top_level` and
  `fusion::filter::tests::clu137_temporal_factors_to_top_level_with_flag` both
  pass.
- New regression test:
  `canonicalization::tests::elides_filter_emptied_by_reduction` directly guards
  the `ReduceScalars` empty-Filter elision.
- `cargo check -p mz-transform` and `cargo fmt` clean.

## Concerns / residual risk

- The change to `ReduceScalars` runs in the flag-off path too. It is a no-op for
  flag-off plans by the argument above, and is corroborated by the full
  flag-off explain-slt and transform-test suites being byte-identical. EXPLAIN
  plan goldens outside `test/sqllogictest/explain` (e.g. some testdrive `.td`
  files) were not exhaustively re-run locally; an empty-Filter elision is a
  semantic no-op so result-based tests are unaffected, but CI plan goldens
  should be watched.
- Deeper underlying gap (not fixed here, out of scope): the eqsat scalar
  canonicalizer is strictly weaker than `reduce` for `isnull(col)` folding via
  column nullability. The empty Filter was the only observed user-visible
  consequence; if eqsat is later expected to fully match `reduce`, an
  isnull/nullability rule would close the gap.
