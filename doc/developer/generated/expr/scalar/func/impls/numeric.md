---
source: src/expr/src/scalar/func/impls/numeric.rs
revision: 0a5fe195ac
---

# mz-expr::scalar::func::impls::numeric

Provides scalar function implementations for the arbitrary-precision `numeric` type: arithmetic, rounding, square root, natural/base-10/base-2 logarithm, power, casts to/from all other numeric types and string, and special-value checks. The `round_numeric` function calls `munge_numeric` after rounding to canonicalize the result, removing negative zero (`-0`) that `dec::Context::round` can produce when rounding a negative fractional value to zero (e.g. `round(-0.4)`).
