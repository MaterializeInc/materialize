---
source: src/expr/src/scalar/func/unary.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::unary

Defines the `LazyUnaryFunc` trait, which all unary scalar function implementations must implement, and which the `derive_unary!` macro delegates to when generating `UnaryFunc`.
The trait requires `eval`, `output_sql_type`, `propagates_nulls`, `introduces_nulls`, `preserves_uniqueness`, `inverse`, `is_monotone`, and `could_error`; these properties drive null-propagation, type inference, and optimizer rewrites.
