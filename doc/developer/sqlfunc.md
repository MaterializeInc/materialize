# The `#[sqlfunc]` macro

The `#[sqlfunc]` attribute macro generates boilerplate for SQL scalar functions.
It creates a unit struct, a trait implementation (`EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc`), and a `Display` impl from a plain Rust function.

The macro lives in `src/expr-derive-impl/src/sqlfunc.rs`.
The generated trait implementations live in `src/expr/src/scalar/func/{unary,binary,variadic}.rs`.

## Quick start

Annotate a function with `#[sqlfunc]`:

```rust
#[sqlfunc]
fn negate_int32(a: i32) -> i32 {
    -a
}
```

This generates:

* A struct `NegateInt32` (camel-cased from the function name).
* An `EagerUnaryFunc` implementation that calls the function.
* A `Display` implementation that prints `"negate_int32"`.
* The original function, unchanged.

## Arity detection

The macro determines function arity from parameter count and types, after excluding `&self` receivers and trailing `&RowArena` parameters:

| Effective params | Dispatches to | Notes |
|---|---|---|
| 0 | Error | Nullary functions are not supported. |
| 1 | `EagerUnaryFunc` | Does not support `&RowArena`. |
| 2 | `EagerBinaryFunc` | Supports `&RowArena`. |
| 3+ | `EagerVariadicFunc` | Supports `&RowArena` and `&self`. |

**Exception:** If any parameter uses `Variadic<T>` or `OptionalArg<T>`, the function is always treated as variadic, regardless of parameter count.

## Modifiers

Pass modifiers as key-value pairs in the attribute:

```rust
#[sqlfunc(sqlname = "+", is_monotone = (true, true), is_infix_op = true)]
fn add_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // ...
}
```

### `sqlname`

The SQL-visible name of the function.

* **Type:** string literal or macro expression
* **Default:** the Rust function name (via `stringify!`)
* **Applies to:** all arities

### `propagates_nulls`

Whether a NULL input produces a NULL output, skipping the function body entirely.
When true, the evaluation layer short-circuits on NULL inputs and returns NULL without calling the function.
This also affects `output_type`: if `propagates_nulls` is true and any input column is nullable, the output column is marked nullable.

* **Type:** boolean expression
* **Default:** inferred from input type — `!Input::nullable()`.
  If the input type accepts `Option` or `Datum` (which can represent NULL), it is nullable, so `propagates_nulls` defaults to `false`.
  If the input type is a non-nullable type like `&str` or `i32`, nulls are propagated (the function never sees them).
* **Applies to:** binary, variadic
* **Not available for:** unary (inferred only, cannot be overridden)

### `introduces_nulls`

Whether the function can produce NULL from non-NULL inputs.
The optimizer uses this to reason about column nullability.

* **Type:** boolean expression
* **Default:** inferred from output type — `Output::nullable()`.
  A function returning `Option<T>` or `Datum` introduces nulls.
  A function returning `String` or `i32` does not.
* **Applies to:** all arities
* **Note:** required when using `output_type_expr` (because the output type is not statically known).

### `could_error`

Whether the function can produce an error at runtime.
The optimizer uses this to avoid certain rewrites that might change error behavior.

* **Type:** boolean expression
* **Default:** inferred from output type — `Output::fallible()`.
  A function returning `Result<T, EvalError>` is fallible.
  A function returning `T` directly is not.
* **Applies to:** all arities

### `is_monotone`

Whether the function is monotone (non-strict; either non-decreasing or non-increasing).
Monotone functions map ranges to ranges: given a range of possible inputs, the range of possible outputs can be determined by mapping the endpoints.

* **Type:** boolean expression for unary and variadic; `(bool, bool)` tuple for binary (one per argument)
* **Default:** `false` for unary and variadic; `(false, false)` for binary
* **Applies to:** all arities

### `preserves_uniqueness`

Whether the function is injective: if `f(x) = f(y)` then `x = y`.

* **Type:** boolean expression
* **Default:** `false`
* **Applies to:** unary only

### `inverse`

The inverse function, if it exists.

* **Type:** expression evaluating to `Option<crate::UnaryFunc>`
* **Default:** `None`
* **Applies to:** unary only

### `negate`

The logical negation of a comparison function.
For example, `<` negates to `>=`.

* **Type:** expression evaluating to `Option<crate::BinaryFunc>`
* **Default:** `None`
* **Applies to:** binary only

### `is_infix_op`

Whether the function is an infix operator (e.g., `+`, `=`, `AND`).
Affects how the function is displayed in EXPLAIN output.

* **Type:** boolean expression
* **Default:** `false`
* **Applies to:** binary, variadic

### `is_associative`

Whether the function is associative: `f(a, f(b, c)) = f(f(a, b), c)`.

* **Type:** boolean expression
* **Default:** `false`
* **Applies to:** variadic only

### `output_type`

An explicit type path for computing the output column type.
When set, the macro generates `output_type` and `introduces_nulls` based on this type instead of inferring from the return type.

* **Type:** type path (e.g., `i16`, `String`)
* **Default:** inferred from return type via `Self::Output::as_column_type()`
* **Applies to:** all arities
* **Cannot be combined with:** `output_type_expr`

### `output_type_expr`

An expression that computes the output column type at runtime.
Use this for functions whose output type depends on input types or struct fields (e.g., `ArrayCreate` where the element type is stored on the struct).

* **Type:** expression evaluating to `SqlColumnType`
* **Default:** none
* **Applies to:** all arities
* **Requires:** `introduces_nulls` (must be specified explicitly)
* **Cannot be combined with:** `output_type`

### `test`

Generate a snapshot test for the macro expansion.

* **Type:** `bool`
* **Default:** `false`
* **Applies to:** all arities

Snapshot files are stored in `src/expr-derive-impl/src/snapshots/`.
Update them with `cargo insta accept` after running `cargo test -p mz-expr-derive-impl`.

## Variadic functions

### Struct name

For variadic functions, the struct name can be specified as the first positional argument.
This is required when a `&self` receiver is present (the struct is defined externally):

```rust
#[sqlfunc(ArrayFill, sqlname = "array_fill")]
fn array_fill_variadic<'a>(&self, fill: Datum<'a>, dims: Datum<'a>, temp_storage: &'a RowArena) -> Result<Datum<'a>, EvalError> {
    // ...
}
```

Without a `&self` receiver, the struct name defaults to the camel-cased function name.
It can still be overridden with the first positional argument:

```rust
#[sqlfunc(Replace, sqlname = "replace")]
fn replace(text: &str, from: &str, to: &str) -> Result<String, EvalError> {
    // ...
}
```

### `&self` receiver

When a `&self` receiver is present, the macro assumes the struct is defined externally and generates:

* A method `impl StructName { fn ... }` containing the function body.
* An `EagerVariadicFunc` trait implementation that delegates to the method.
* A `Display` implementation.

Without `&self`, the macro generates the struct itself (with standard derives) in addition to the trait and display implementations.

### `Variadic<T>` and `OptionalArg<T>`

These wrapper types affect both arity detection and null handling.

`Variadic<T>` consumes all remaining arguments from the iterator.
It wraps a `Vec<T>` and is used for functions with a truly variable number of arguments:

```rust
#[sqlfunc(is_associative = true)]
fn concat(strs: Variadic<Option<&str>>) -> Result<String, EvalError> {
    // strs is a Vec<Option<&str>>, one entry per SQL argument
}
```

`OptionalArg<T>` consumes one argument if present, or produces `None` if the iterator is exhausted.
It wraps an `Option<T>` and is used for functions with optional trailing arguments:

```rust
#[sqlfunc(sqlname = "lpad")]
fn pad_leading(string: &str, raw_len: i32, pad: OptionalArg<&str>) -> Result<String, EvalError> {
    let pad = pad.unwrap_or(" ");
    // ...
}
```

Both are defined in `src/repr/src/scalar.rs`.

### `&RowArena`

A trailing `&RowArena` parameter gives the function access to temporary storage for allocating return values that borrow from the arena.
It is excluded from arity detection and from the generated `Input` type.
The arena is always passed to binary and variadic `call` implementations (the trait requires it); for functions that don't use it, the parameter is simply unused.

Unary functions do not support `&RowArena`.

### Input type

For variadic functions, the generated `Input` type depends on parameter count:

* **Single parameter:** the bare type (e.g., `Variadic<Option<&'a str>>`).
* **Multiple parameters:** a tuple (e.g., `(&'a str, &'a str, &'a str)`).

## Null handling

The interplay between `propagates_nulls`, `introduces_nulls`, and input/output types determines the nullability of the output column.

The `output_type` method on each generated struct computes the output `SqlColumnType` as:

```
output.nullable = output.nullable || (propagates_nulls && any_input_nullable)
```

Where:
* `output.nullable` comes from `introduces_nulls` (or is inferred from the output type).
* `propagates_nulls` indicates whether NULL passes through.
* `any_input_nullable` is true if any input column is nullable.

The evaluation layer (`LazyUnaryFunc`, `LazyBinaryFunc`, `LazyVariadicFunc`) handles the actual null short-circuiting at runtime through the `InputDatumType::try_from_result`/`try_from_iter` methods.
If the input type does not accept NULL (is non-nullable), the evaluation layer returns NULL directly without calling the function.

## Examples

### Unary: type cast with inverse

```rust
#[sqlfunc(
    sqlname = "uint2_to_real",
    preserves_uniqueness = true,
    inverse = to_unary!(super::CastFloat32ToUint16),
    is_monotone = true
)]
fn cast_uint16_to_float32(a: u16) -> f32 {
    f32::from(a)
}
```

### Binary: arithmetic operator

```rust
#[sqlfunc(
    is_monotone = (true, true),
    output_type = i16,
    is_infix_op = true,
    sqlname = "+",
    propagates_nulls = true,
)]
fn add_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    a.unwrap_int16()
        .checked_add(b.unwrap_int16())
        .ok_or(EvalError::NumericFieldOverflow)
        .map(Datum::from)
}
```

### Variadic: fixed parameters

```rust
#[sqlfunc(sqlname = "datediff")]
fn date_diff_date(unit_str: &str, a: Date, b: Date) -> Result<i64, EvalError> {
    // Three fixed parameters → variadic with tuple input (&str, Date, Date)
}
```

### Variadic: dynamic argument count

```rust
#[sqlfunc(is_associative = true)]
fn concat(strs: Variadic<Option<&str>>) -> Result<String, EvalError> {
    // Variadic<T> detected → variadic dispatch regardless of parameter count
}
```

### Variadic: with `&self` and dynamic output type

```rust
#[sqlfunc(
    ArrayCreate,
    sqlname = "array_create",
    output_type_expr = "SqlScalarType::Array(Box::new(self.elem_type.clone())).nullable(false)",
    introduces_nulls = false
)]
fn array_create<'a>(&self, datums: Variadic<Datum<'a>>, temp_storage: &'a RowArena) -> Array<'a> {
    // &self: struct defined externally with an elem_type field
    // output_type_expr: output type depends on runtime struct fields
}
```
