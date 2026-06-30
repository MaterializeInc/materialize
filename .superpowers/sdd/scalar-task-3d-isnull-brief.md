# Task 3d: IS NULL fold (close the eqsat nullability gap)

Phase 3 of the scalar equality-saturation canonicalizer. Add the `IsNull` fold so
the eqsat engine matches `MirScalarExpr::reduce`'s primary IS NULL simplification
at the source, instead of relying on the downstream `ReduceScalars` empty-Filter
catch added in commit 06741f1419. This was the capability gap behind the 3c
empty-Filter regression. Depends on the 1h contract.

## The rewrite

```text
IsNull(x) -> false   when x is non-nullable
```

Source: `reduce_pre`'s IsNull arm (`src/expr/src/scalar/reduce.rs:58-65`):
```rust
UnaryFunc::IsNull(_) => {
    if !expr.typ(column_types).nullable {
        *e = MirScalarExpr::literal_false();
    } else if let Some(rewritten) = expr.decompose_is_null() { ... }
}
```

SCOPE: implement ONLY the non-nullable fold (`IsNull(non-nullable) -> false`).
DEFER `decompose_is_null` (the nullable -> disjunction-of-IsNulls rewrite) to a
later task; note the deferral in your report.

## Nullability source

reduce uses `expr.typ(column_types).nullable`. We have `col_types` in the
ScalarEGraph (since 1e) and the same `raise + typ` machinery `call_scalar_type`
uses. Compute the operand's nullability as
`raise::raise(eg, operand).typ(eg.col_types()).nullable`. Do NOT add a new
nullability e-class analysis for this task (on-demand typ matches reduce and is
bounded, like `if_err_cond` / `call_scalar_type` already raise mid-saturation).

For the CLU-137-adjacent regression case the non-nullability comes from the
column type in `col_types` at the call site (a join-equivalence makes the column
non-nullable in the input type passed to `canonicalize_predicates`), so the
`col_types`-based `typ().nullable` sees it.

## Soundness: gate on could_error (DELIBERATE deviation from reduce)

reduce folds `IsNull(non-nullable) -> false` UNGATED on errors. But `IsNull`
evaluates its operand first and propagates an operand ERROR (IsNull(err) = err,
not false). So folding to `false` when the operand can error is NOT exact-eval
sound: at a row where the operand errors, the input is `err` while `false` is not.
This is the same envelope-vs-exact-eval situation as the 1e/1f rules. Our
canonicalizer holds the conservative exact-eval contract (guarded by the
differential test), so:

* FIRE `IsNull(x) -> false` only when x is non-nullable
  (`raise(x).typ(col_types).nullable == false`) AND
  `eg.analysis(x).could_error == false`.

When both hold, x is never null and never errors, so `IsNull(x)` is always
`false`. Exact-eval sound. This is strictly more conservative than reduce. For the
regression case x is a column (could_error == false, non-nullable), so it fires
and matches reduce. State this gate + rationale in the doc comment.

## The rule

Add `isnull_fold` to `rules.rs`, registered in `rules()`. For
`node = CallUnary { func, expr }`:
1. Return `vec![]` unless `func` is `UnaryFunc::IsNull(_)`.
2. If `eg.analysis(*expr).could_error` is true, return `vec![]` (gate).
3. Compute `nullable = raise::raise(eg, *expr).typ(eg.col_types()).nullable`. If
   `nullable`, return `vec![]`.
4. Otherwise return `vec![ eg.add(SNode::Literal(false-literal)) ]` (build the
   `false` boolean literal the same way the other rules build literals, e.g. via
   `destructure_literal(MirScalarExpr::literal_false())`).

Borrow discipline (1h): finish the analysis/raise reads before `eg.add`.

## Tests (rules.rs test module, existing style)

1. FIRES on non-nullable, error-free operand: `IsNull(col)` with col_types marking
   that column NON-nullable -> canonicalizes to `false`. (Pass col_types with
   `.nullable(false)`.) Pair with an eval check.
2. DOES NOT fire on a nullable operand: `IsNull(col)` with the column nullable ->
   stays `IsNull(col)` (not folded). Differential over null and non-null rows.
3. GATE blocks on an erroring operand (the deviation from reduce): `IsNull(1/c0)`
   where `1/c0` is non-nullable-typed but could_error (division) -> must NOT fold
   to false; differential incl. c0==0 (operand errors -> IsNull errors), proving
   folding to false would be unsound there. Assert it is not `false`.
4. Composition: `NOT(IsNull(col))` with col non-nullable error-free -> `true`
   (isnull_fold + not + const-fold), mirroring the regression predicate
   `(#1) IS NOT NULL` collapsing.
5. Confirm `test_canonicalize_eval_differential` still passes.

## Optional re-verification (nice to have, not required for the task)

If cheap, note that with this rule the 3c equi-join empty-Filter no longer needs
the ReduceScalars catch (eqsat folds the IS NOT NULL itself). Do NOT remove the
ReduceScalars fix (it is a correct general improvement and defense in depth).

## Constraints (binding)

* Separate scalar engine; no relational code. No `as`. No em-dashes /
  clause-joining semicolons in comments.
* Doc comment: the rewrite, the could_error gate AND why (IsNull propagates
  operand errors, so exact-eval soundness needs the gate; deviation from reduce's
  ungated fold), the nullability source (col_types via raise+typ, mirrors reduce),
  and that decompose_is_null is deferred.

## Done criteria

`cargo check`/`clippy -p mz-transform --tests` clean; `bin/fmt` applied (consult
the `mz-test` skill for the test command). All prior scalar tests pass; new
isnull tests pass; differential passes. Report the new total.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar IsNull-of-non-nullable fold, could_error-gated (Phase 3)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-3d-isnull-report.md`.
Return only: status, commit sha, one-line test summary (new total), what you
deferred (decompose_is_null), concerns.
