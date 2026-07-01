# Task 1e: thread col_types + null-propagation rules

Part of Phase 1 of the scalar equality-saturation canonicalizer. Two pieces:
(1) thread `col_types` into the canonicalizer so rules can compute result types,
(2) add null-propagation rules for binary and variadic calls. Error propagation
is a separate later task (1f). De Morgan and the If-Err arm remain deferred.

## Why col_types is needed now

Null propagation rewrites `f(null, x)` to a typed null literal
`literal_null(result_scalar_type)`. The result type of a call over a non-literal
operand depends on the column types, so the canonicalizer must thread
`col_types`. Constant folding did not need this (a fully-literal subtree has no
columns), which is why earlier tasks omitted it. The 1b review flagged this exact
upcoming signature change.

## Part 1: thread col_types

* `ScalarEGraph` (egraph.rs): add a field `col_types: Vec<ReprColumnType>`.
  Keep `new()` returning an empty `col_types` (existing direct-`new()` tests do
  not need types). Add `pub fn with_col_types(col_types: Vec<ReprColumnType>) -> Self`
  and `pub fn col_types(&self) -> &[ReprColumnType]`.
* `canonicalize` (scalar.rs): change the signature to
  `pub fn canonicalize(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> MirScalarExpr`.
  It builds the e-graph via `ScalarEGraph::with_col_types(col_types.to_vec())`,
  then lowers/saturates/raises as before.
* Update existing `canonicalize(&expr)` call sites (all in the scalar test
  modules) to pass `&[]`. This is sound for them: the const-fold, boolean, NOT,
  and If rules never type a column, so an empty `col_types` is never consulted on
  those paths. Add a one-line comment at one such call site noting why `&[]` is
  acceptable there.

Do NOT change `lower`/`raise`/`node`/`analysis` signatures. `col_types` lives on
the e-graph and is read by rules through `eg.col_types()`.

## Part 2: the result-type helper

Add to rules.rs:

```rust
/// The scalar type a call node evaluates to, computed from its children's types.
///
/// Rules that build a typed literal (a null or an error) need the call's output
/// type. We reconstruct the call as a `MirScalarExpr` by raising each child to
/// its cheapest representative and asking `typ`, which is what reduce does with
/// `e.typ(column_types).scalar_type`.
fn call_scalar_type(eg: &ScalarEGraph, node: &SNode) -> ReprScalarType { ... }
```

Implement by raising each child id (`raise::raise(eg, child)`), assembling the
parent `MirScalarExpr` (unary/binary/variadic/if) from the node's func and the
raised children, and returning `assembled.typ(eg.col_types()).scalar_type`. The
raised children may carry columns, which is why `eg.col_types()` is needed.

## Part 3: the null-propagation rules and the soundness gate

reduce's binary null-prop (`src/expr/src/scalar/reduce/binary.rs:42`) and
variadic null-prop (`src/expr/src/scalar/reduce/variadic.rs:50`) are:

* binary: `(e1.is_literal_null() || e2.is_literal_null()) && func.propagates_nulls()`
  -> `literal_null(e.typ.scalar_type)`.
* variadic: `func.propagates_nulls() && exprs.any(is_literal_null)`
  -> `literal_null(e.typ.scalar_type)`.

### DELIBERATE DEVIATION FROM REDUCE (read carefully)

reduce applies null-prop UNGATED on the other operands. That is unsound for our
non-destructive canonicalizer, and we deviate. A probe established that
Materialize's eval returns the ERROR, not null, for `f(null, x)` when `x` errors:
`eval(AddInt64(null, 1/0))` is `Err(DivisionByZero)`, not `Null`. So rewriting
`f(null, x)` to null when `x` can error would change `err` into `null`, an
unsound rewrite (and in the e-graph it would union a null class with the
error-producing class, which is worse).

Therefore null-prop here fires ONLY when every OTHER operand cannot error:

* binary `f(a, b)` with `a` literal-null: fire iff `func.propagates_nulls()` and
  `eg.analysis(b).could_error == false`. Symmetric if `b` is the literal-null.
* variadic `f(exprs)`: fire iff `func.propagates_nulls()`, some operand is
  literal-null, and EVERY non-null-literal operand has `could_error == false`.

This is strictly more conservative than reduce. It never changes `err` to `null`,
and it makes null-prop and const_fold agree on the all-literal case (an
error-literal operand has `could_error == true`, so the gate blocks null-prop and
lets const_fold produce the error), so the two rules never union disagreeing
values. Document this reasoning in the rule doc comments, citing the probe
result. Do NOT use the ungated reduce form.

Note there is no unary null-prop rule: a unary call with a literal-null operand
is fully literal, so const_fold already handles it.

### Helpers

* `is_literal_null(eg, id) -> bool`: `eg.analysis(id).literal` is
  `Some((Ok(row), _))` with `row.unpack_first() == Datum::Null`.

### Rules to add to `rules()`

* `null_prop_binary`: as above, produces `SNode::Literal` of
  `literal_null(call_scalar_type(eg, node))`.
* `null_prop_variadic`: as above.

## Constraints (binding)

* Separate scalar engine; no relational code.
* No `as` conversions.
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract,
  reasoning inline at the decision point.
* Non-destructive; rule signature unchanged.
* Null-prop MUST be could_error-gated as specified (deviation from reduce). Do
  not ship the ungated form.

## Tests (property-based where possible; the soundness gate)

Use explicit integer rows (the bool_cube is for boolean columns; null-prop tests
use an int column in arithmetic). `eval_owned` already exists.

1. `null_prop_binary` fires on a safe operand: `AddInt64(null_int, c0)` with
   `col_types = [Int64 nullable]`. `canonicalize` returns `literal_null(Int64)`.
   Differential: for `c0` in a few `Datum::Int64` values and `Datum::Null`,
   `eval` of input and output match (both null, since Add propagates nulls and c0
   cannot error). Assert BOTH the structural result and the differential.
2. `null_prop_binary` does NOT fire when the other operand can error:
   `AddInt64(null_int, 1 / c0)` with `col_types = [Int64 nullable]`. `1 / c0` has
   `could_error == true`, so the gate blocks null-prop. Assert
   `canonicalize(&expr, ct) != literal_null(...)` (the rule did not fire; the
   result is still the call). Differential including `c0 == 0`: `eval(input)` is
   `Err` there, and the unchanged output evals to the same `Err`. This is the
   test that proves the gate prevents the unsound err->null. Also include a
   non-error row (`c0 == 5`) where both eval to null.
3. `null_prop_variadic` fires on safe operands and does NOT fire when a non-null
   operand can error. Pick a propagates_nulls variadic func (confirm one;
   coalesce does NOT propagate nulls, so use a different one, e.g. construct via
   a variadic arithmetic-style func if available, otherwise document that no
   suitable safe variadic exists and cover binary only, reporting this).
4. Keep all prior scalar tests green (you changed the canonicalize signature).

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* New null-prop tests pass; all prior scalar tests pass with the updated
  signature.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: thread col_types and add could_error-gated null propagation (Phase 1)`.
End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1e-report.md` and return
only: status, commit sha, one-line test summary, concerns. Note whether you
found a suitable propagates_nulls variadic func for test 3.
