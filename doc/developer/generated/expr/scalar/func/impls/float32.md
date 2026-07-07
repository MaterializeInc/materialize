---
source: src/expr/src/scalar/func/impls/float32.rs
revision: abff67db9b
---

# mz-expr::scalar::func::impls::float32

Provides scalar function implementations for `float4` datums: arithmetic, casts to/from other numeric types and string, rounding, trigonometry, and IEEE-754 special-value checks.
The `cast_float32_to_uint32` and `cast_float32_to_uint64` functions use a strict upper bound (`<` rather than `<=`) when checking the rounded value against the target type's maximum, because `u32::MAX` and `u64::MAX` are not exactly representable as `f32` and both round up to the next power of two, which the subsequent `as` cast would silently saturate to the maximum value instead of returning an out-of-range error.
