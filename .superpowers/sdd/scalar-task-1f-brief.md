# Task 1f: scalar error-propagation rules (could_error-gated)

Part of Phase 1 of the scalar equality-saturation canonicalizer. Adds error
propagation for binary and variadic calls, reusing the col_types and analysis
infrastructure from 1e. Mirrors 1e's null-propagation structure and its
deliberate could_error gate. No infrastructure changes.

## Goal

When a call has a literal-error operand, the call evaluates to an error, so fold
it to that error literal:

* `null_prop`-style binary: `f(err_lit, b)` or `f(a, err_lit)` -> the error literal.
* variadic: `f(.., err_lit, ..)` -> the error literal.

## Source of truth and the deliberate could_error gate

reduce's error propagation: binary (`src/expr/src/scalar/reduce/binary.rs:46-55`)
checks `expr1.as_literal_err()` then `expr2.as_literal_err()`, UNGATED. Variadic
(`src/expr/src/scalar/reduce/variadic.rs:60`) gates on `propagates_nulls()`.

We DEVIATE the same way 1e did, for the same reasons:

* MIR does not guarantee WHICH error a multi-operand call surfaces, and eval is
  left-to-right via `?`. `eval(f(x, err_lit))` returns `x`'s error if `x` errors
  (reached first), not the literal error. So an ungated err-prop could change one
  error into a different error, which would (a) fail our exact-eval-differential
  tests and (b) in the e-graph union two different error classes.
* Therefore err-prop fires on a literal-error operand only when EVERY OTHER
  operand has `could_error == false`. Then no other operand can produce a
  competing error and the call's result is EXACTLY the literal error, regardless
  of the function's argument-evaluation order. This is position-independent and
  robust, and matches the 1e null-prop gate.

This is strictly more conservative than reduce. Document it in the rule doc
comments, referencing the 1e null-prop precedent and the same probe finding
(error wins over null; eval is left-to-right with `?`).

### No conflict with const_fold or null_prop

* All-literal call with an error operand: the error operand has
  `could_error == true`. If it is the ONLY potentially-erroring operand, the gate
  passes and err-prop fires -> the error literal. const_fold also fires (all
  literal) and evaluates to the same error (eval surfaces the error). They agree.
  If TWO operands could error, the gate blocks err-prop and only const_fold fires.
  No disagreeing union either way.
* err-prop and null-prop never both fire: if an operand is a literal error,
  null-prop's gate (other operands could_error == false) is violated by that
  error operand, so null-prop does not fire. Symmetric.

## Verified API (from 1e, reuse as-is)

* `eg.analysis(id).could_error` and `eg.analysis(id).literal`.
* `eg.col_types()`, and the `call_scalar_type(eg, node) -> ReprScalarType` helper
  already in rules.rs (raises children, assembles the call, returns
  `.typ(col_types).scalar_type`).
* `is_literal_null(eg, id)` exists. Add a sibling `fn literal_err(eg, id) -> Option<EvalError>`
  that returns the error when the class is a literal error
  (`eg.analysis(id).literal` is `Some((Err(e), _))`), else `None`.

The result type is `call_scalar_type(eg, node)`, and the error literal is
`MirScalarExpr::literal(Err(err), result_type)` destructured into `SNode::Literal`
(reuse `destructure_literal`).

## Rules to add to `rules()`

* `err_prop_binary`: match `CallBinary{expr1, expr2, ..}`. If `expr1` is a literal
  error and `eg.analysis(expr2).could_error == false`, fire with `expr1`'s error.
  Else if `expr2` is a literal error and `eg.analysis(expr1).could_error == false`,
  fire with `expr2`'s error. Produce `SNode::Literal(Err(err), call_scalar_type)`.
  (No `propagates_nulls` gate for binary, matching reduce; the could_error gate is
  what makes it sound.)
* `err_prop_variadic`: match `CallVariadic{exprs, ..}`. If some operand is a
  literal error AND every OTHER operand has `could_error == false`, fire with that
  operand's error. (Mirror reduce's `propagates_nulls` gate here too: only fire
  when `func.propagates_nulls()`, to match reduce's variadic behavior; combine
  with the could_error gate.)

If multiple operands are literal errors, all other operands are themselves errors
(could_error true), so the gate blocks the rule; const_fold handles that
all-literal case. So you only ever fire with a single well-defined error.

## Constraints (binding)

* Separate scalar engine; no relational code.
* No `as` conversions.
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract,
  reasoning inline.
* Non-destructive; rule signature unchanged.
* err-prop MUST be could_error-gated as specified. Do not ship the ungated form.

## Tests (exact-error eval-differential, the soundness gate)

Use explicit integer rows and `eval_owned` (exact comparison, including the error
value). A folded constant error is easy to obtain: `lit_int(1) / lit_int(0)`
const-folds to a `Literal(Err(DivisionByZero))`, so build error operands that
way, or construct the error literal directly.

1. `err_prop_binary` fires (first position): `AddInt64(div(1,0), c0)` with
   `col_types = [Int64 nullable]`, `c0` a column (could_error false). Folds to the
   error literal. Assert `canonicalize == literal(Err(DivisionByZero), Int64)` and
   the exact-error differential over a few `c0` rows (all yield that error).
2. `err_prop_binary` fires (second position): `AddInt64(c0, div(1,0))`. Same.
3. `err_prop_binary` BLOCKED when the other operand can error:
   `AddInt64(1/c0, div(1,0))` with `c0` a column. `1/c0` has could_error true, so
   the gate blocks. Assert the rule did NOT fire (result is still a CallBinary,
   not a bare error literal) and the differential holds at `c0 == 0` (where
   `1/c0` errors first) and `c0 == 5`. This proves the gate preserves the exact
   error.
4. `err_prop_variadic` fires and is blocked analogously, using a
   `propagates_nulls` variadic (the 1e tests found `VariadicFunc::MakeTimestamp`;
   reuse it). If no clean variadic case is constructible, cover binary thoroughly
   and report it.
5. All prior scalar tests stay green.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* New err-prop tests pass; all prior scalar tests pass.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: could_error-gated error propagation (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1f-report.md` and return
only: status, commit sha, one-line test summary, concerns.
