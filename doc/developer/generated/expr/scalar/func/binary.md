---
source: src/expr/src/scalar/func/binary.rs
revision: d08f8f74a0
---

# mz-expr::scalar::func::binary

Defines the `LazyBinaryFunc` and `EagerBinaryFunc` traits, used by all binary scalar function implementations and delegated to by the `derive_binary!` macro.
`LazyBinaryFunc` requires `eval`, `output_sql_type`, `propagates_nulls`, `introduces_nulls`, `is_infix_op`, `negate`, `could_error`, `is_monotone`, and `is_infinity_monotone`, covering type inference and optimizer properties such as infix display and negation rewriting.
`is_infinity_monotone` defaults to `true` and returns `false` for multiplication and division: their indeterminate forms (`0 * inf`, `inf / inf`) and magnitude collapse (`finite / inf = 0`) produce results the range endpoints do not bound, so the abstract interpreter must not narrow their output range when an operand may be infinite.
The blanket `impl<T: EagerBinaryFunc> LazyBinaryFunc for T` delegates `is_infinity_monotone` from `EagerBinaryFunc` to `LazyBinaryFunc`.
