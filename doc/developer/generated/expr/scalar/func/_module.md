---
source: src/expr/src/scalar/func.rs
revision: 90a38f32be
---

# mz-expr::scalar::func

The central module for all scalar function definitions in `mz-expr`.
The root `func.rs` file defines many arithmetic, string, regex, timezone, and cryptographic functions directly using `#[sqlfunc]`, plus shared helpers like `parse_timezone`, `build_regex`, `stringify_datum`, `regexp_match_static`, `regexp_split_to_array_re`, and `array_create_scalar`.
Submodules define the function enum infrastructure and per-type implementations:
- `macros` -- `derive_unary\!`, `derive_binary\!`, `derive_variadic\!`, and `to_unary\!` code-generation macros.
- `unary` -- `LazyUnaryFunc`, `EagerUnaryFunc` traits and the `UnaryFunc` enum.
- `binary` -- `LazyBinaryFunc`, `EagerBinaryFunc` traits and the `BinaryFunc` enum.
- `variadic` -- `LazyVariadicFunc`, `EagerVariadicFunc` traits and the `VariadicFunc` enum.
- `impls` -- per-type function implementations (one submodule per SQL scalar type).
- `unmaterializable` -- `UnmaterializableFunc` for session-context functions.
- `format` -- date/time formatting.
- `encoding` -- binary encoding.

`UnaryFunc`, `BinaryFunc`, `VariadicFunc`, and `UnmaterializableFunc` are re-exported and referenced by `MirScalarExpr`.
