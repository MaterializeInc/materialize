# Task 3d report: IS NULL fold

## Status

DONE. `isnull_fold` added to the scalar equality-saturation rule set
(`src/transform/src/eqsat/scalar/rules.rs`), registered in `rules()` after the
err-prop rules.

## The rewrite

```text
IsNull(x) -> false   when x is non-nullable AND error-free
```

Mirrors the non-nullable arm of `reduce_pre`'s IsNull rewrite
(`src/expr/src/scalar/reduce.rs`). Nullability is computed on demand via the same
`raise + typ` machinery `call_scalar_type` uses:
`raise::raise(eg, *expr).typ(eg.col_types()).nullable`. No new e-class analysis
was added. The `false` literal is built with
`destructure_literal(MirScalarExpr::literal_false())`. Borrow discipline holds:
the `could_error` read and the `raise + typ` read both finish (returning owned
values) before `eg.add`.

## could_error gate (deliberate deviation from reduce)

reduce folds this arm UNGATED on errors. `IsNull` evaluates its operand first and
propagates an operand error (`IsNull(err) = err`, not `false`), so folding to
`false` when the operand can error is not exact-eval sound. The rule therefore
fires only when `eg.analysis(*expr).could_error == false` AND x is non-nullable.
When both hold, x is never null and never errors, so `IsNull(x)` is always
`false`. Strictly more conservative than reduce, consistent with the 1e/1f rules.
For the regression case x is a column (error-free, non-nullable), so it fires.

## Tests

4 new `#[mz_ore::test]`s in the rules test module, all paired with eval
differentials:

1. `test_isnull_fold_fires_on_non_nullable_col` — non-nullable error-free col ->
   `false`.
2. `test_isnull_fold_blocked_on_nullable_col` — nullable col stays `IsNull`;
   differential over null and non-null rows.
3. `test_isnull_fold_blocked_when_operand_can_error` — `IsNull(1/c0)` with c0
   non-nullable (operand non-nullable-typed but could_error) does NOT fold;
   differential incl. c0 == 0 where the input is Err, proving the gate is needed.
4. `test_isnull_fold_composition_not_is_not_null` — `NOT(IsNull(col))` with col
   non-nullable error-free -> `true`, mirroring the regression's IS NOT NULL.

Test command: `cargo nextest run -p mz-transform eqsat::scalar`. New total: 87
scalar tests, all passing (was 83), including `test_canonicalize_eval_differential`
(300 cases). `cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt`
applied.

## Deferred

`decompose_is_null` (reduce's nullable -> disjunction-of-IsNulls rewrite). Out of
scope per the brief; noted in the `isnull_fold` doc comment.

## Concerns

None. The ReduceScalars empty-Filter fix (commit 06741f1419) was left untouched
as a correct general improvement and defense in depth. I did not re-run the full
slt EXPLAIN survey to confirm the equi-join empty-Filter no longer needs that
catch (the optional re-verification); the unit-level composition test covers the
IS NOT NULL collapse path.
