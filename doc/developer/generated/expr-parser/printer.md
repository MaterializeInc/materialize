---
source: src/expr-parser/src/printer.rs
revision: 94ee2d5448
---

# mz-expr-parser::printer

Renders a `MirScalarExpr` to text in the syntax accepted by the expr-parser's own `try_parse_scalar`. Parsing the output reconstructs an equal expression, so the module provides a round-trip guarantee for the expression types it supports.

## Key export

**`print_scalar(expr: &MirScalarExpr) -> Result<String, String>`** — the primary entry point. Returns `Err` for datum types or error literal variants the text syntax cannot represent (for example, non-internal `EvalError` variants or scalar types not covered by `print_repr_scalar_type`).

## Rendering rules

- **`Column(i)`** → `#i`
- **`Literal(Ok(row), typ)`** → delegates to `print_literal`, which formats datums with explicit type casts (`1::integer`, `"hello"`, `true`, etc.).
- **`Literal(Err(Internal(msg)))`** → `error("internal error: ...")`. Other error variants are unsupported.
- **`CallUnmaterializable(func)`** → `func()`
- **`CallUnary`** — `IsNull` renders as `((expr) IS NULL)`. `RecordGet[i]` renders as `record_get[i](expr)`. `CastInt32ToNumeric` with a scale renders as `cast_int32_to_numeric[scale](expr)`. All other unary functions render as `variant_name(expr)`.
- **`CallBinary`** — comparison operators with a unique variant (`Eq`, `NotEq`, `Lt`, `Lte`, `Gt`, `Gte`) render as infix `(expr1 op expr2)`. All others render as `variant_name(expr1, expr2)`.
- **`CallVariadic`** — `And`/`Or` with at least two arguments and no nested call of the same function render as infix `(a AND b ...)` / `(a OR b ...)`. `RecordCreate` renders as `record_create["field1", ...](args)`. `ListCreate` renders as `list_create[elem_type](args)`. All others render as `variant_name(args)`.
- **`If { cond, then, els }`** → `case when cond then then else els end`

Unlike the EXPLAIN printer, this renderer uses exact variant names (via `variant_name()`) rather than SQL operator symbols, so the round-trip through the parser is lossless.
