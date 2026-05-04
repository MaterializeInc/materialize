# expr::scalar::func

The complete scalar function library for MIR. Defines three public function
enums (`UnaryFunc`, `BinaryFunc`, `VariadicFunc`) and `UnmaterializableFunc`,
all referenced by `MirScalarExpr`. Also exposes helper functions used in
both `impls/` and `variadic.rs` (e.g. `parse_timezone`, `build_regex`,
`stringify_datum`, `array_create_scalar`).

## Files (12,392 LOC, including impls/)

| File | What it owns |
|---|---|
| `func.rs` (3,184) | Shared helpers + module declarations + re-exports; 7 `#[sqlfunc]` fns inline |
| `variadic.rs` (1,780) | `LazyVariadicFunc`/`EagerVariadicFunc` traits, 30+ variadic impls inline, `derive_variadic!` invocation |
| `format.rs` (947) | Date/time format string parsing and rendering (`DateTimeFormat`) |
| `binary.rs` (695) | `LazyBinaryFunc`/`EagerBinaryFunc` traits, `derive_binary!` invocation (222 variants) |
| `unary.rs` (663) | `LazyUnaryFunc`/`EagerUnaryFunc` traits, `derive_unary!` invocation (329 variants) |
| `macros.rs` (425) | `derive_unary!`, `derive_binary!`, `derive_variadic!` declarative macros |
| `encoding.rs` (217) | Binary encoding/decoding (base64, hex, escape) |
| `unmaterializable.rs` (144) | `UnmaterializableFunc` enum (session-/env-dependent functions) |
| `impls.rs` (76) | Re-export shim for `impls/` submodules |
| `impls/` (7,445) | Per-type implementations *(see `impls/CONTEXT.md`)* |

## Key concepts

- **`LazyXxxFunc` / `EagerXxxFunc` trait pair** — for each arity: `Eager`
  expresses a function via typed `Input`/`Output` associated types; a blanket
  impl provides `LazyXxxFunc` for free, deriving null-propagation and error
  properties from the type signatures. Functions needing short-circuit
  evaluation (e.g., `And`, `If`) implement `LazyXxxFunc` directly.
- **`derive_unary!` / `derive_binary!` / `derive_variadic!` macros** — each
  generates the enum, a delegating `impl` block, a `Display` impl, and
  `From<Variant>` impls for all listed types. Marked "temporary" pending full
  migration to `enum_dispatch`. The unary enum has 329 variants; binary has
  222; variadic has ~30.
- **`#[sqlfunc]` proc-macro** — defined in `mz-expr-derive`; generates a
  newtype and an `EagerUnaryFunc` impl from a plain Rust `fn`.
- **`UnmaterializableFunc`** — functions (e.g., `MzNow`, `CurrentTimestamp`)
  that cannot be folded at planning time; evaluated by `mz-adapter`.

## Bubbled in from impls/

- Per-type implementations use `#[sqlfunc]` for simple cases and manual
  `LazyUnaryFunc` impls for casts whose type-inference depends on `mz-sql::typeconv`
  (which is outside this crate); those sites carry `TODO? if typeconv was in expr`
  comments and conservatively return `introduces_nulls = true`.

## Cross-references

- `mz-expr-derive` provides `#[sqlfunc]`.
- `MirScalarExpr` (in `scalar.rs`) holds `CallUnary { func: UnaryFunc }`,
  `CallBinary { func: BinaryFunc }`, `CallVariadic { func: VariadicFunc }`.
- `mz-transform` consumes `is_monotone`, `preserves_uniqueness`, `inverse`,
  `negate` for optimizer rewrites.
- Generated docs: `doc/developer/generated/expr/scalar/func/`.
