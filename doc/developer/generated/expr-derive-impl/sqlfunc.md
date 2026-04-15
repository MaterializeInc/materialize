---
source: src/expr-derive-impl/src/sqlfunc.rs
revision: 5680493e7d
---

# mz-expr-derive-impl::sqlfunc

Implements the `#[sqlfunc]` proc-macro expansion.
Classifies annotated functions by arity (unary, binary, variadic) via `determine_arity` and generates the corresponding `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc` trait impl, a unit struct (or method impl for `&self` receivers), `Display`, and standard derives, all driven by the `Modifiers` struct parsed via `darling::FromMeta` from macro attributes.
`Modifiers` fields include: `sqlname`, `is_monotone`, `preserves_uniqueness`, `inverse`, `negate`, `is_infix_op`, `output_type`, `output_type_expr`, `could_error`, `propagates_nulls`, `introduces_nulls`, `is_associative`, `is_eliminable_cast`, and `test`.
The `SqlName` helper enum supports both literal strings and macro expressions for function display names.
Helper functions handle type patching, nullability inference, arena detection, and optional `insta` snapshot test generation for each arity pattern.
