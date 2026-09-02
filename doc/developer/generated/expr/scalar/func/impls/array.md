---
source: src/expr/src/scalar/func/impls/array.rs
revision: b28da7d561
---

# mz-expr::scalar::func::impls::array

Provides scalar function implementations for array datums.
Key types:
- `CastArrayToListOneDim` -- casts a 1-D array to a list (errors on multi-dimensional input), generated via `#[sqlfunc]`. This cast does not preserve uniqueness: it returns only the array's elements and drops dimension metadata, so arrays that differ only in their lower bounds (e.g. `[1:1]={42}` and `[2:2]={42}`) collapse to the same list.
- `CastArrayToString` -- converts an array to text via `stringify_datum`, parameterized by `SqlScalarType`.
- `CastArrayToJsonb<E = MirScalarExpr>` -- converts an array (including multi-dimensional) to a JSONB value, applying a per-element `cast_element: Box<E>` expression. This cast does not preserve uniqueness: JSONB arrays reconstruct nested arrays from dimension lengths only and carry no lower bounds, so arrays that differ only in their lower bounds produce the same JSONB value.
- `CastArrayToArray<E = MirScalarExpr>` -- element-wise cast between two array types using a `cast_expr: Box<E>` and `return_ty`.
All manual structs implement `LazyUnaryFunc` directly. `CastArrayToJsonb` and `CastArrayToArray` each expose `try_map_expr` and `map_expr` methods for converting their inner expression type to another (used when lowering from MIR to LIR).
