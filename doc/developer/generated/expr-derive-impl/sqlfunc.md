---
source: src/expr-derive-impl/src/sqlfunc.rs
revision: ad1150f222
---

# mz-expr-derive-impl::sqlfunc

Implements the `#[sqlfunc]` proc-macro expansion.
Classifies annotated functions by arity (unary, binary, variadic) via `determine_arity` and generates the corresponding `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc` trait impl, a unit struct (or method impl for `&self` receivers), `Display`, standard derives, and a `cfg_attr`-gated `proptest_derive::Arbitrary` derive (active under `test` or the `proptest` feature), all driven by the `Modifiers` struct parsed via `darling::FromMeta` from macro attributes.
`Modifiers` fields include: `sqlname`, `is_monotone`, `preserves_uniqueness`, `inverse`, `negate`, `is_infix_op`, `output_type`, `output_type_expr`, `could_error`, `propagates_nulls`, `introduces_nulls`, `is_associative`, `is_eliminable_cast`, `is_infinity_monotone`, and `test`.
For binary functions, if `is_infinity_monotone` is set, the macro generates an `is_infinity_monotone` method on the `EagerBinaryFunc` impl. For unary and variadic functions, the field is accepted but ignored. This attribute marks multiplication and division functions as non-infinity-monotone, informing the abstract interpreter not to apply endpoint-based range narrowing when an operand may be infinite.
The `SqlName` helper enum supports both literal strings and macro expressions for function display names.
For each generated unit struct (unary, binary, and variadic), the macro also emits a `FuncName` trait impl (`impl crate::func::FuncName for #struct_name`) with a `NAME` constant set to the function's identifier via `stringify!`, plus (under the `func-registry` feature) a `SQLFUNC` const of type `Option<SqlFuncSource>` and a `sqlfunc_input_types` method. `SqlFuncSource` captures the full `#[sqlfunc]` declaration text, a types-only signature string, and an FNV-1a fingerprint of the function body; both the declaration and signature are rendered from token streams via `render_tokens` so that formatting and comments are invisible to them.
The macro auto-derives `output_type_expr` for functions with generic type parameters when none is provided explicitly: it classifies how each generic appears in the input and output types (`GenericUsage`: `Bare`, `InContainer`, or `Absent`) and emits calls to `SqlContainerType::unwrap_element_type` and `wrap_element_type` as needed. For multi-input functions it also emits `mz_ore::soft_assert_or_log!` consistency checks ensuring all inputs carrying the same generic agree on the SQL element type.
Helper functions handle type patching, nullability inference, arena detection, lifetime elision, generic-parameter erasure (`Datum<'a>`), and optional `insta` snapshot test generation for each arity pattern.
