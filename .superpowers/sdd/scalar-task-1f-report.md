# Task 1f report: could_error-gated error propagation

## Status

DONE

## Commit

`a5c276a880`

## Test summary

52/52 scalar tests pass; 5 new tests added: err_prop_binary fires in first
and second position, blocked when other operand can error (binary), variadic
fires, variadic blocked when other can error.

## What was done

Added `literal_err(eg, id) -> Option<EvalError>` helper (sibling to
`is_literal_null`) plus two rules:

* `err_prop_binary`: fires when one operand of a `CallBinary` is a literal
  error and the other has `could_error == false`.
  No `propagates_nulls` gate (matching reduce's binary err-prop, which is
  unconditional on the function).
* `err_prop_variadic`: fires when one operand of a `CallVariadic` is a
  literal error and every other operand has `could_error == false`.
  Also gates on `func.propagates_nulls()` (matching reduce's variadic
  err-prop).

Both deviate from reduce's ungated form for the same reason as 1e null-prop:
left-to-right eval means an ungated rule could substitute one error class for
a different one. The gate ensures the literal error is the only possible
error, so the call's result is exactly that error.

The `EvalError` import was added to the existing `use mz_expr::{...}` line.
All checks passed (`cargo check`, `cargo clippy`, `cargo fmt`, `bin/lint`
excluding the pre-existing `.superpowers/sdd/*.md` whitespace failure).

## Concerns

None.
