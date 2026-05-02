---
source: src/expr/src/scalar/func/binary.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::binary

Defines the `LazyBinaryFunc` trait, used by all binary scalar function implementations and delegated to by the `derive_binary!` macro.
Requires `eval`, `output_sql_type`, `propagates_nulls`, `introduces_nulls`, `is_infix_op`, `negate`, `could_error`, and `is_monotone`, covering type inference and optimizer properties such as infix display and negation rewriting.
