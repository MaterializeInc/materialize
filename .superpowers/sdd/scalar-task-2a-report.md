# Task 2a report: undistribute factoring rule (residual-error gated)

Status: DONE
Commit: bade736500 "eqsat: scalar undistribute factoring rule, residual-error gated (Phase 2)"

## What was implemented

`factor_and_or` in `src/transform/src/eqsat/scalar/rules.rs`, registered in
`rules()` between `and_or_drop_unit` and `not_not`.

The rule, for a `CallVariadic` with `is_and_or(outer_func)` and >= 2 operands:

1. `inner_func = outer_func.switch_and_or()`.
2. Per outer operand, compute its inner-operand set as sorted/unique canonical
   ids: if the operand's class holds an `inner_func` variadic node, use its
   operands (`eg.find` each); else treat the operand as a singleton (reduce's
   1-arg wrapping). Scans `eg.nodes(canon)` like `not_not`.
3. `intersection` = retain across all branch sets; empty -> `vec![]`.
4. Per-branch residual = set minus intersection; ANY empty residual -> `vec![]`
   (the absorption case, left to 2b, keeps factoring distinct).
5. RESIDUAL-error gate: any residual operand with `could_error` -> `vec![]`. The
   common factor is NOT gated (the precise narrowing of reduce's
   `self.could_error()`; CLU-137's fallible cast lives in the factor).
6. Build, after reading all sets into owned values: per-branch combination node
   (singleton residual reused as-is), `residual_combination` under `outer_func`,
   `factored` = `inner_func(intersection.. , residual_combination)`. Sorted ids
   for hashcons stability. Returns `vec![factored]`.

Full-intersection only (one step over all branches), non-destructive, no
fixpoint loop, no heuristic. Reuses `is_and_or` / `switch_and_or`.

## Tests (all 5 from the brief, in the rules.rs test module)

- `test_factor_and_or_soundness`: both duals over the 3VL cube.
- `test_factor_and_or_fires`: extracts `c0∧(c1∨c2)` and the dual (cheaper form).
- `test_factor_and_or_erroring_common_factor`: erroring common factor `g` is
  pulled out; differential over c0==0 (g errors) stays exact-eval equal.
- `test_factor_and_or_gate_blocks_erroring_residual`: gate blocks an erroring
  residual; includes the explicit witness (c0=null, c1=0, c2=true) showing the
  input errors while the unsound factored form would mask it to null.
- `test_factor_and_or_absorption_not_fired_here`: empty-residual case left
  untouched, not collapsed to `c0∧c1`.

Added a recursive `normalize` test helper (sorts AND/OR operands) for
commutative-order-insensitive comparison.

## Verification

- `cargo clippy -p mz-transform --tests`: clean.
- `bin/fmt`: applied, clean.
- `cargo nextest run -p mz-transform eqsat::scalar`: 66 passed (61 prior
  unchanged + 5 new), including `test_canonicalize_eval_differential`.

New scalar test total: 66.

## Deferred (as instructed)

- Subset factoring (reduce's `indexes_to_undistribute` / `else` branch).
- The second independently-sound gate disjunct "common factor cannot be null",
  which needs a nullability analysis we do not have. Adding it would broaden
  applicability to cases where residuals can error.

## Concerns

None. Soundness of the residual-error gate was checked by hand over the 3VL +
error cases for both duals (factor error/null with error-free residuals), and
the phase-level differential exercises the rule over random nested terms.
