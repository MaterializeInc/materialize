---
source: src/expr/src/scalar/func/impls/boolean.rs
revision: 75abef4839
---

# mz-expr::scalar::func::impls::boolean

Provides scalar function implementations for boolean datums: `NOT`, casts from/to string, and casts from/to integer types.
All functions use `#[sqlfunc]` and declare properties such as `preserves_uniqueness`, `inverse`, and `is_monotone`.
