# Task 1e report: thread col_types + could_error-gated null propagation

Status: DONE

## What changed

* `egraph.rs`: added a `col_types: Vec<ReprColumnType>` field to `ScalarEGraph`,
  plus `with_col_types(col_types)` constructor and `col_types()` accessor.
  `new()`/`default()` still produce an empty `col_types`.
* `scalar.rs`: changed `canonicalize` to
  `canonicalize(expr: &MirScalarExpr, col_types: &[ReprColumnType])`. It builds
  the e-graph via `ScalarEGraph::with_col_types(col_types.to_vec())`. The
  `canonicalize_is_identity` call site was updated to pass `&[]` with a comment
  explaining why that is sound.
* `rules.rs`:
  * Updated every existing test call site to pass `&[]` (sound: the boolean,
    NOT, If, and const-fold rules never type a column). `assert_eval_equiv`
    passes `&[]` internally with a comment.
  * Added `call_scalar_type(eg, node)`: raises each child to its cheapest
    representative, reassembles the parent `MirScalarExpr`, and returns
    `assembled.typ(eg.col_types()).scalar_type` (mirrors reduce).
  * Added `is_literal_null(eg, id)`: true iff the class's literal analysis is
    `Some((Ok(row), _))` whose single datum is `Datum::Null`.
  * Added `null_prop_binary` and `null_prop_variadic`, both registered in
    `rules()`.

## The soundness gate (deviation from reduce)

reduce applies null-prop ungated on the other operands. We do NOT. The rules
fire only when `func.propagates_nulls()` AND every OTHER operand has
`could_error == false` (read via `eg.analysis(id).could_error`). This is
strictly more conservative than reduce. The doc comments on both rules cite the
probe result `eval(AddInt64(null, 1/0)) == Err(DivisionByZero)` (error wins over
null in eval), explain why the ungated form would turn `err` into `null`, and
note that the gate makes null-prop agree with `const_fold` on the all-literal
case (an error literal has `could_error == true`, so the gate blocks null-prop
and lets const_fold emit the error). No unary null-prop rule: a unary call over
a literal-null operand is fully literal and `const_fold` handles it.

## Tests

Five new tests, all under `eqsat::scalar::rules::tests`:

1. `test_null_prop_binary_fires_on_safe_operand`: `AddInt64(null, c0)` with
   `[Int64 nullable]` collapses to `literal_null(Int64)`; differential over
   `{0, 7, -3, Null}` matches input.
2. `test_null_prop_binary_blocked_when_other_can_error`: `AddInt64(null, 1/c0)`;
   the rule does NOT fire (result stays a `CallBinary`); differential including
   `c0 == 0` (input evals to `Err`) and `c0 == 5` (null) matches the unchanged
   output. Asserts the input is `Err` at `c0 == 0` to make the err->null hazard
   explicit. This is the gate-proving test.
3. `test_null_prop_variadic_fires_on_safe_operands` and
   `test_null_prop_variadic_blocked_when_other_can_error`: same shape over a
   variadic func (see below).

Test 3 variadic func: I found a suitable `propagates_nulls` variadic,
`VariadicFunc::MakeTimestamp` (SQL `makets`). Its six operands (five `i64`, one
`f64`) are all non-nullable, so `propagates_nulls()` is `true`; the fires-test
asserts that at runtime. The func is itself fallible, which does not affect the
gate (the gate checks only the OTHER operands), so it exercises both the firing
and the blocked path cleanly. `Coalesce`, `Greatest`, and `Least` all return
`propagates_nulls() == false`, as the brief warned.

## Checks

* `cargo check -p mz-transform --tests`: clean.
* `cargo clippy -p mz-transform --tests`: clean.
* `cargo fmt -p mz-transform`: applied.
* `cargo test -p mz-transform --lib eqsat::scalar`: 47 passed, 0 failed.

## Concerns

None blocking. One note: `MakeTimestamp` produces a timestamp output, so the
variadic differential compares timestamp/null rows rather than ints, which is
fine since `eval_owned` compares packed `Row`s by value. A pure-int
`propagates_nulls` variadic does not exist in the catalog, so `MakeTimestamp` is
the cleanest available choice.
