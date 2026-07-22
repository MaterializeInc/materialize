---
source: src/expr/src/scalar/func/impls/float64.rs
revision: 2668c60445
---

# mz-expr::scalar::func::impls::float64

Provides scalar function implementations for `float8` datums: arithmetic, casts to/from numeric types and string, rounding, trigonometric and logarithmic functions, and IEEE-754 special-value checks.
`cast_float64_to_uint64` uses a strict upper bound (`<` rather than `<=`) when comparing the rounded value against `u64::MAX as f64`, because `u64::MAX` is not exactly representable as an `f64` and rounds up to 2^64; a `<=` bound would admit 2^64 and the subsequent `as u64` cast would saturate it to `u64::MAX` instead of returning an error.
The `mz_sleep` function returns `Result<Option<...>, EvalError>` and uses `Duration::try_from_secs_f64` to validate its argument, returning `EvalError::InvalidParameterValue` for invalid values (negative, NaN, overflow) rather than panicking.
