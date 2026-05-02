---
source: src/expr/src/scalar/func/impls/varchar.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::varchar

Provides a single scalar function implementation for the `varchar` type: `CastVarCharToString`, which unwraps the inner `&str` from a `VarChar` value.
Declared with `preserves_uniqueness = true`, `is_eliminable_cast = true`, and an inverse pointing to `CastStringToVarChar`.
