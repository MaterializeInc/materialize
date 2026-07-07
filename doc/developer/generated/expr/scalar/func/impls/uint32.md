---
source: src/expr/src/scalar/func/impls/uint32.rs
revision: 609c6b372d
---

# mz-expr::scalar::func::impls::uint32

Provides scalar function implementations for Materialize's `uint4` (unsigned 32-bit integer): arithmetic with overflow checks and casts to/from other numeric types and string.
The narrowing casts `uint4_to_uint2`, `uint4_to_smallint`, and `uint4_to_integer` are marked `preserves_uniqueness = false` because they error on out-of-range input values; marking them as uniqueness-preserving would allow cast inversion to rewrite `col::target_type = lit` into an index lookup on `col`, silently dropping the range-check error.
