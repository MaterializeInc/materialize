# Task 1i Report: De Morgan Rule (Phase 1)

## Status: DONE

## Commit

`7d21177a7d` — `eqsat: scalar De Morgan rule over AND/OR (Phase 1)`

## Implementation

Added `not_demorgan` to `src/transform/src/eqsat/scalar/rules.rs`:

- Matches `SNode::CallUnary { func: Not, expr }`.
- Calls `eg.nodes(*expr)` to get an owned `Vec<SNode>` (releases borrow) before any `eg.add()` calls.
- For each child `SNode::CallVariadic { func: vf, exprs }` where `is_and_or(&vf)`:
  - Clones `vf.switch_and_or()` and `func.clone()` (Not) before mutation.
  - Builds `NOT(operand)` for each operand via `eg.add(SNode::CallUnary { ... })`.
  - Builds the flipped variadic via `eg.add(SNode::CallVariadic { func: new_func, exprs: not_ids })`.
- Registered in `rules()` between `not_binary_negate` and `if_true`.

Doc comment states the no-gate soundness rationale: structural equivalence in Kleene three-valued logic, operand-order-preserving, short-circuit-value-preserving via `switch_and_or`, mirrors `reduce::demorgans` (ungated).

## Tests

Three tests added:

1. **`test_not_demorgan_soundness`**: `assert_eval_equiv` over the {true, false, null} cube for `NOT(AND(c0, c1))` and `NOT(OR(c0, c1))`.

2. **`test_not_demorgan_fires_via_not_not`**: Proves the rule fired via cost-reducing composition. `NOT(AND(NOT c0, NOT c1))` canonicalizes to `OR(c0, c1)` (cost 3 vs 6), and `NOT(OR(NOT c0, NOT c1))` canonicalizes to `AND(c0, c1)`. Each assertion is paired with `assert_eval_equiv`.

3. **`test_not_demorgan_error_preservation`**: Uses `d = (1/c0 == 5)` (errors at c0=0). `NOT(AND(NOT d, NOT c1))` canonicalizes to `OR(d, c1)` (cost 7 vs 10). Differential over c0 in {0, 5} x c1 in {true, false, null}. The `is_err()` assertion uses c0=0, c1=false: at that row `AND(NOT d=Err, NOT c1=true)` returns Err (true does not short-circuit past the error), then `NOT(Err)=Err`.

## Error preservation note

The brief suggested `c1=true` for the `is_err()` assertion, but that row gives `true` because `AND(Err, false)` short-circuits on false (And is a `LazyVariadicFunc`). The fix: use c1=false where `NOT c1 = true`, so `AND(Err, true)` does not short-circuit and returns Err. The differential still covers all six rows including both error rows.

## Test count

59 scalar tests pass (56 prior + 3 new). Full mz-transform suite: 257 pass, 1 skipped (unchanged from baseline 254 + 3).

## Checks

- `cargo check -p mz-transform --tests`: clean
- `cargo clippy -p mz-transform --tests`: clean
- `bin/fmt`: clean
- All 56 prior scalar tests: pass unchanged
