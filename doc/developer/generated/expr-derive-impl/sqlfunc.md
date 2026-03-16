---
source: src/expr-derive-impl/src/sqlfunc.rs
revision: 9c1e2767b0
---

# mz-expr-derive-impl::sqlfunc

Implements the `#[sqlfunc]` proc-macro expansion.
Classifies annotated functions by arity (unary, binary, variadic) and generates the corresponding `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc` trait impl, a unit struct (or method impl for `&self` receivers), `Display`, and derives, all driven by the `Modifiers` struct parsed from macro attributes.
Helper functions handle type patching, nullability inference, and `SqlName` parsing; snapshot tests exercise each arity pattern.
