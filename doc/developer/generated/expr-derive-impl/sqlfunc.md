---
source: src/expr-derive-impl/src/sqlfunc.rs
revision: 94ee2d5448
---

# mz-expr-derive-impl::sqlfunc

Implements the `#[sqlfunc]` proc-macro expansion.
Classifies annotated functions by arity (unary, binary, variadic) via `determine_arity` and generates the corresponding `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc` trait impl, a unit struct (or method impl for `&self` receivers), `Display`, standard derives, and a `cfg_attr`-gated `proptest_derive::Arbitrary` derive (active under `test` or the `proptest` feature), all driven by the `Modifiers` struct parsed via `darling::FromMeta` from macro attributes.
`Modifiers` fields include: `sqlname`, `is_monotone`, `preserves_uniqueness`, `inverse`, `negate`, `is_infix_op`, `output_type`, `output_type_expr`, `could_error`, `propagates_nulls`, `introduces_nulls`, `is_associative`, `is_eliminable_cast`, `is_infinity_monotone`, and `test`.
For binary functions, if `is_infinity_monotone` is set, the macro generates an `is_infinity_monotone` method on the `EagerBinaryFunc` impl. For unary and variadic functions, the field is accepted but ignored. This attribute marks multiplication and division functions as non-infinity-monotone, informing the abstract interpreter not to apply endpoint-based range narrowing when an operand may be infinite.
The `SqlName` helper enum supports both literal strings and macro expressions for function display names.
For each generated unit struct (unary, binary, and variadic), the macro also emits a `FuncName` trait impl (`impl crate::func::FuncName for #struct_name`) with a `NAME` constant set to the function's identifier via `stringify!`.
Helper functions handle type patching, nullability inference, arena detection, and optional `insta` snapshot test generation for each arity pattern.
