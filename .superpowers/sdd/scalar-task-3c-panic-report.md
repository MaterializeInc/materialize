# Phase 3c finding: eqsat scalar canonicalizer evaluates `mz_panic` during optimization

## Status
FIXED and verified.

## Severity: YES (serious)
A NORMAL, non-BROKEN query whose predicate contains `mz_panic` aborts the
optimizer with the flag on. Demonstrated empirically:

```
SELECT * FROM t WHERE mz_unsafe.mz_panic('boom') = 'x'
```

With `enable_eqsat_scalar_canonicalize` defaulted ON and no guard, this query
fails optimization. The panic backtrace originates directly in eqsat:

```
12: mz_transform::eqsat::scalar::rules::const_fold
13: ScalarEGraph::saturate
14: eqsat::scalar::canonicalize
15: eqsat::scalar::canonicalize_predicates::{closure#0}
16: mz_expr::relation::canonicalize::canonicalize_predicates_with
17: fusion::filter::Filter::action
...
26: optimize::peek::Optimizer::optimize     <- a plain SELECT/peek
```

`catch_unwind_optimize` converts the panic into
`internal error: ... unexpected panic during query optimization: boom`, so the
query fails to optimize instead of producing a plan that panics at RUNTIME
(the intended `mz_panic` semantics, which flag-off preserves via reduce's guard).

## Root cause
`const_fold` (src/transform/src/eqsat/scalar/rules.rs) folds any call whose
operands are all literals by calling `call.eval(..)`. For a literal-argument
`CallUnary { Panic, .. }` that runs `mz_panic`, which `panic!`s
(src/expr/src/scalar/func/impls/string.rs:1253) inside saturation. `reduce`
deliberately excludes `Panic` from constant folding (reduce/unary.rs:26); eqsat
const_fold had no such guard. The canonicalizer only runs on FILTER PREDICATES
(injected into step 1 of `canonicalize_predicates`), so the trigger is a
predicate-shaped `mz_panic`, not the SELECT-projection `mz_panic` of
broken_statements.slt.

## Fix
Added the Panic guard to `const_fold`: skip folding a `CallUnary` whose func is
`UnaryFunc::Panic(_)`, mirroring reduce. Minimal, no `as`, no flag changes,
reduce untouched.

## Goldens regenerated
- test/sqllogictest/transform/predicate_reduction.slt: added one EXPLAIN
  regression for `WHERE mz_panic('boom') = 'x'`, golden generated with the fix.
  The plan keeps the call in the filter (`Filter ("x" = mz_panic("boom"))`),
  proving it is planned not evaluated. This is a NEW test, not a regression of
  an existing golden.
- broken_statements.slt: NOT touched and NOT regenerated. It is projection-only,
  the canonicalizer never sees it, and it already PASSES flag-on (14/14). Its
  `mz_panic` panic fires via FoldConstants on a Map and is caught by the BROKEN
  mechanism, yielding the committed `stage optimize/global not present` message.
  The earlier survey attribution of broken_statements to this finding was a
  misattribution: the real failure is the predicate path above.

## Verification
- New unit test `test_no_fold_panic` (direct `Panic(lit)` and nested
  `Eq(Panic(lit),'x')`): PASS. Without the guard this test panics.
- eqsat::scalar suite (88 tests incl. test_canonicalize_eval_differential): PASS.
- predicate_reduction.slt: 16/16 PASS (with fix); the same query fails-to-optimize
  without the fix (severity confirmed by revert-rebuild-rerun).
- broken_statements.slt: 14/14 PASS flag-on.
- cargo clippy -p mz-transform --tests: clean. bin/fmt: clean.

## Concerns
- `isnull_fold` folds `IsNull(Panic(lit))` to `false` (elides, does not evaluate).
  This matches reduce (which also folds IsNull-of-non-nullable to false), so it
  is not a divergence introduced here, but it does turn a would-be runtime panic
  into `false`. Pre-existing and out of scope; noted for awareness.

## Report path
.superpowers/sdd/scalar-task-3c-panic-report.md
