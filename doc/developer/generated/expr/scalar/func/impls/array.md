---
source: src/expr/src/scalar/func/impls/array.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::array

Provides scalar function implementations for array datums.
Key types:
- `CastArrayToListOneDim` -- casts a 1-D array to a list (errors on multi-dimensional input), generated via `#[sqlfunc]`.
- `CastArrayToString` -- converts an array to text via `stringify_datum`, parameterized by `SqlScalarType`.
- `CastArrayToJsonb` -- converts an array (including multi-dimensional) to a JSONB value, applying a per-element `cast_element` expression.
- `CastArrayToArray` -- element-wise cast between two array types using a `cast_expr` and `return_ty`.
All manual structs implement `LazyUnaryFunc` directly.
