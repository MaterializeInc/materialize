---
source: src/expr/src/scalar/eval.rs
revision: 5d046b3ab6
---

# mz-expr::scalar::eval

Defines the `Eval` trait, a generic interface for types that can evaluate to a `Datum`.

The trait has two methods:

- `eval` — takes a slice of `Datum` values (column references for the current row) and a `RowArena` for temporary allocation, and returns either a `Datum<'a>` or an `EvalError`. Implementations should not panic on malformed input; instead they should return an appropriate `EvalError`.
- `could_error` — returns `true` if evaluation could possibly produce an error when given non-error input datums. Used by the optimizer to decide whether to hoist or eliminate expressions.

`MirScalarExpr` implements this trait. The trait is re-exported from `mz_expr::scalar` alongside the `Columns` trait.
