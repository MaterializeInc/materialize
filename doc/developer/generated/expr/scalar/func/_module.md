---
source: src/expr/src/scalar/func.rs
revision: 83e8931660
---

# mz-expr::scalar::func

The central module for all scalar function definitions in `mz-expr`.
Contains the root `func.rs` file (which defines many arithmetic, string, regex, timezone, and cryptographic functions directly using `#[sqlfunc]`, plus shared helpers like `parse_timezone`, `build_regex`, and `stringify_datum`) and submodules that together define `UnaryFunc`, `BinaryFunc`, `VariadicFunc`, and `UnmaterializableFunc`.
Key submodules: `unary`/`binary`/`variadic` define the lazy-eval traits and use `macros.rs` to generate the corresponding enums; `impls` contains per-type function implementations; `format` handles date/time formatting; `encoding` handles binary encoding; `unmaterializable` handles session-context functions.
`UnaryFunc`, `BinaryFunc`, `VariadicFunc`, and `UnmaterializableFunc` are all re-exported from this module and are the primary types referenced by `MirScalarExpr`.
