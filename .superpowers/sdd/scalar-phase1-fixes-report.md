# Phase 1 review fixes report (scalar eqsat canonicalizer)

Branch `claude/mir-equality-optimizer-sodbej`. All five batch items applied,
plus two bug fixes the new differential test (item 1) forced.

## The five batch items

1. **Phase-level differential test** (`rules.rs` test module,
   `test_canonicalize_eval_differential`). A deterministic seeded xorshift64 RNG
   drives a type-directed recursive generator of well-typed `MirScalarExpr` over
   c0/c1 (Int64) and c2/c3 (Bool): columns, int/bool/null literals, binary
   Add/Mul/Div and Eq/NotEq, variadic And/Or, unary Not, and If. Boolean-context
   operands are bool-typed, arithmetic operands int-typed, If conds bool and
   branches share a type, so every term is well-typed. 3 fixed seeds x 100 exprs
   = 300 cases, each differentialed `eval_owned(e, row)` vs
   `eval_owned(canonicalize(e, col_types), row)` over a 225-row datum cube (int
   cols hold 0 and i64::MAX so DivInt64/overflow errors fire, bool cols range
   over {true,false,null}). Results are `Result`s, compared by value, so an
   error->null, error->other-error, or wrong-value rewrite fails. Corpus size is
   asserted. Generator biases toward nested calls (1/4 leaf-stop), depth 2..=4.

2. **`find_in` dedup** (`egraph.rs`). Extracted `fn find_in(uf: &[Id], id: Id)
   -> Id` and replaced the three inlined union-find loops (`find`, the `add`
   analysis closure, the `rebuild` analysis closure). Identical non-compressing
   semantics.

3. **Distinct-branch if_err_cond test** (`rules.rs`,
   `test_if_err_cond_silent_on_distinct_branches`). `if(c0, c1, c2)`: distinct
   branches so if_same_branches cannot mask if_err_cond staying silent; asserts
   the If round-trips unchanged. Existing if(c0,c1,c1) test kept.

4. **merge literal-conflict debug_assert** (`analysis.rs`). `merge` now
   `debug_assert!`s that two `Some` literals agree. Release behavior unchanged.

5. **not_demorgan doc tightened** (`rules.rs`). Now attributes error correctness
   to the order-INDEPENDENT `max` over operand errors (`func/variadic.rs:89`),
   not to left-to-right evaluation.

## Two bug fixes the differential test surfaced (flagged for review)

The differential is the soundness gate over rule interactions, and it found two
genuine pre-existing defects. Both are minimal, localized, well-documented fixes.
Without them the mandated test cannot pass, and neutering the test would hide the
defects.

- **A: `if_same_branches` was unsound w.r.t. condition errors** (`rules.rs`).
  `if(c, x, x) -> x` dropped a condition error: `If` evaluates the condition
  first via `?`, so when `c` can error the input surfaces that error while `x`
  does not. Repro found: `if(MulInt64(c1,2)=..., null, null)` collapsed to `null`
  but evals to `Err(NumericFieldOverflow)` at c1=i64::MAX. reduce's `then==els`
  arm (`reduce/if_then.rs:37`) is itself ungated, but this canonicalizer's
  contract is strict eval-equivalence (the whole could_error machinery exists for
  this). Fix: gate the rule on `!eg.analysis(cond).could_error`, the same gate
  and rationale as the err/null-prop rules. Strictly more conservative than
  reduce.

- **B: `raise` could panic on cyclic / mid-saturation graphs** (`raise.rs`).
  Two issues: (1) `node_cost` looked up child costs by raw child id, but costs
  are keyed by canonical id; mid-saturation raise (if_err_cond/call_scalar_type
  run between unions, before the next rebuild) sees stale child ids, so the lookup
  missed and a costed class looked uncosted. (2) The single-pass DFS cost walk
  was not a fixpoint, so a class whose only finite derivation routes through a
  class still on the DFS stack could be left uncosted and panic in `build`. Fix:
  canonicalize child ids in `node_cost`, and replace the DFS with a
  least-fixpoint relaxation that is provably total (every class holds at least
  its lowered finite subtree). This subsumes the reviewer's deferred raise
  cycle-guard concerns.

## Verification

- `cargo check -p mz-transform --tests`: clean.
- `cargo clippy -p mz-transform --tests`: clean.
- `bin/fmt`: applied.
- `cargo nextest run -p mz-transform`: 259 passed, 0 failed.
- Scalar tests: 61 passed (prior 59 + differential + distinct-branch).
- Differential: 300 random expressions x 225 rows = 67,500 evaluations,
  deterministic (fixed seeds), passes.

## Constraints honored

Separate scalar engine only; no `as` conversions; no em-dashes / clause-joining
semicolons in comments; matched existing style.
