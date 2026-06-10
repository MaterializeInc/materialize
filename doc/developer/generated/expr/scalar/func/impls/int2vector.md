---
source: src/expr/src/scalar/func/impls/int2vector.rs
revision: cd1934f510
---

# mz-expr::scalar::func::impls::int2vector

Provides scalar function implementations for the PostgreSQL `int2vector` type.
- `CastInt2VectorToArray` -- unwraps the inner `Array` from an `Int2Vector`, marked as an eliminable cast with `is_monotone = true`.
- `CastInt2VectorToString` -- converts an `Int2Vector` to its text representation using `stringify_datum`, with an inverse to `CastStringToInt2Vector`.
