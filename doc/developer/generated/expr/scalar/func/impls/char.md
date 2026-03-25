---
source: src/expr/src/scalar/func/impls/char.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::char

Provides scalar function implementations for the fixed-length `char(n)` type.
Key types:
- `PadChar` -- restores blank padding to a stored (trimmed) char value up to the declared `CharLength`, implementing `EagerUnaryFunc` directly.
- `CastCharToString` -- unwraps the inner `&str` from a `Char` value, marked as `preserves_uniqueness = true`, `is_eliminable_cast = true`, with an inverse to `CastStringToChar`.
