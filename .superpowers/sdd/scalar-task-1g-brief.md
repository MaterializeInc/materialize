# Task 1g: scalar If-error-condition rule

Part of Phase 1 of the scalar equality-saturation canonicalizer. Adds the one
remaining If-resolution case deferred from 1d: a literal-error condition. It uses
the col_types infrastructure from 1e. No infrastructure changes. One rule.

## Goal

`if(err_cond, then, els) -> the error`, where `err_cond` is a literal error. This
completes the If-resolution family (1d did true/false/null/same-branches).

## Source of truth and why it is unconditionally sound

reduce's If-Err arm (`src/expr/src/scalar/reduce/if_then.rs`, the `Err(err)` case
of `cond.as_literal()`):

```rust
Err(err) => {
    *e = MirScalarExpr::Literal(
        Err(err.clone()),
        then.typ(column_types).union(&els.typ(column_types)).unwrap(),
    );
}
```

This rule needs NO could_error gate, unlike the null/error-propagation rules.
`If` eval evaluates the condition FIRST and short-circuits on it
(`scalar.rs` Eval: `match cond.eval(datums, temp)? { ... }`). A literal-error
condition makes `cond.eval()?` return the error before `then` or `els` are ever
evaluated, so `eval(if(err_cond, t, e))` is exactly that error regardless of what
`t` and `e` are or whether they could error. The rewrite to that error literal is
therefore unconditionally sound.

## Verified API (reuse)

* `eg.analysis(id)` for `could_error` / `literal`. `literal_err(eg, id) -> Option<EvalError>`
  already exists (added in 1f): returns the error when the class is a literal
  error.
* `eg.col_types()`; `raise::raise(eg, id) -> MirScalarExpr`.
* `SNode::If { cond, then, els }`.
* `destructure_literal(MirScalarExpr) -> SNode`.

`call_scalar_type` returns only the scalar type. This rule needs the full
`ReprColumnType` of `then` and `els` (to union their nullabilities too), so do not
reuse `call_scalar_type` directly. Compute each branch's type by raising it and
calling `.typ(eg.col_types())`, then union:

```rust
let then_ty = raise::raise(eg, *then).typ(eg.col_types());
let els_ty = raise::raise(eg, *els).typ(eg.col_types());
let Some(result_ty) = then_ty.union(&els_ty) else {
    // Incompatible branch types should not occur in a well-typed expression.
    // Be conservative and do not fire rather than unwrap-panic like reduce.
    return vec![];
};
```

(`ReprColumnType::union(&self, other) -> Result<ReprColumnType, _>` or
`Option<...>`. Confirm the exact return shape and adapt the `let ... else`. reduce
calls `.unwrap()` on it.)

## The rule

* `if_err_cond`: match `SNode::If { cond, then, els }`. If `literal_err(eg, *cond)`
  is `Some(err)`, produce `SNode::Literal(Err(err), result_ty)` via
  `MirScalarExpr::literal(Err(err), result_ty.scalar_type)` ... NOTE: check whether
  the literal must carry the full `ReprColumnType` (`result_ty`) or just the
  scalar type. `SNode::Literal` holds `(Result<Row, EvalError>, ReprColumnType)`,
  so build it with the full `result_ty`. reduce builds `Literal(Err, result_ty)`
  with the full column type, so match that: construct the `SNode::Literal`
  directly as `SNode::Literal(Err(err), result_ty)` rather than via
  `MirScalarExpr::literal` (which takes a scalar type). Mirror how the error
  literal's type is built in reduce exactly.

Add `if_err_cond` to the `rules()` list.

## Constraints (binding)

* Separate scalar engine; no relational code.
* No `as` conversions.
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract,
  reasoning inline (note the unconditional-soundness reason: cond evaluated first).
* Non-destructive; rule signature unchanged.
* No could_error gate (it is unconditionally sound, document why).

## Tests (eval-differential)

The condition must be a literal error so the rule fires; the branches should be
columns or otherwise non-trivial so const_fold does not collapse the whole If.

1. `if_err_cond` fires: `if(div(1,0), c0, c1)` with `col_types = [Int64 nullable, Int64 nullable]`.
   `div(1,0)` const-folds to a literal error, so after saturation the If has a
   literal-error condition. `canonicalize` returns the error literal
   `Literal(Err(DivisionByZero), <union type>)`. Assert it is a `Literal(Err(_), _)`
   and that its error is `DivisionByZero`. Differential: for several int rows,
   `eval(input)` and `eval(output)` are both that error (the condition errors
   first regardless of c0/c1).
2. Branch types unioned: pick `then`/`els` with differing nullability (e.g. a
   nullable column and a non-null literal) and confirm `canonicalize` does not
   panic and produces a well-typed error literal. (A light check; the main point
   is no panic and the result is the error.)
3. Does NOT fire on a non-error condition: `if(c0, c1, c2)` with `c0` a column is
   untouched by this rule (covered by 1d rules / left as the If). Confirm
   `if(true_lit, ...)`/`if(false_lit, ...)` still resolve via the 1d rules (a
   regression check that 1g did not break them).
4. All prior scalar tests stay green.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* New test(s) pass; all prior scalar tests pass.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar If-error-condition rule (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1g-report.md` and return
only: status, commit sha, one-line test summary, concerns (including the exact
`ReprColumnType::union` return shape you found).
