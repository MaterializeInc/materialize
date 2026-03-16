---
source: src/expr/src/scalar/func/impls.rs
revision: e757b4d11b
---

# mz-expr::scalar::func::impls

Re-exports all type-specific scalar function implementations, organized into one submodule per SQL type (e.g., `boolean`, `int32`, `timestamp`, `string`).
Each submodule uses `#[sqlfunc]`-annotated functions that are collected into the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums by `macros.rs`.
The breadth of submodules (32 files) reflects the full set of built-in scalar types supported by Materialize's SQL dialect.
