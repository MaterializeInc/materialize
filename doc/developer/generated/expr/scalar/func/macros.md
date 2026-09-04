---
source: src/expr/src/scalar/func/macros.rs
revision: b28da7d561
---

# mz-expr::scalar::func::macros

Defines the `derive_unary\!`, `derive_binary\!`, and `derive_variadic\!` macros that generate the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums and their delegating `impl` blocks.
`derive_unary!` generates `UnaryFunc<E = MirScalarExpr>` — a generic enum where variants whose inner type carries an expression field are instantiated as `VariantName<E>`, while expression-free variants remain unit-like. It emits delegating methods (`eval`, `output_sql_type`, `output_type`, `propagates_nulls`, `introduces_nulls`, `could_error`, `is_monotone`, etc.) under `impl<E: Eval> UnaryFunc<E>`, plus `try_map_expr` and `map_expr` methods under `impl<E> UnaryFunc<E>` for converting the enum's expression type (used when lowering `UnaryFunc<MirScalarExpr>` to `UnaryFunc<LirScalarExpr>`). `derive_binary!` and `derive_variadic!` take a list of variant-name/inner-type pairs and emit the enum definition (with standard derives), delegating methods, a `Display` impl, and `From<InnerType>` conversions for each variant.
`derive_binary!` additionally delegates `is_infinity_monotone` to `LazyBinaryFunc::is_infinity_monotone` on each variant, making it callable as `BinaryFunc::is_infinity_monotone(&self)`.
Each generated enum also exposes three methods for test tooling:
- `variant_name(&self) -> &'static str` -- returns the canonical name of the active variant, as declared by its `FuncName` impl.
- `from_variant_name(name: &str) -> Option<Self>` -- constructs a variant from its canonical name via JSON deserialization; only works for unit-like variants whose inner type has no required fields.
- `variant_names() -> impl Iterator<Item = &'static str>` -- iterates all canonical names in declaration order.
Also contains a `to_unary\!` helper macro used by individual scalar function implementations to express their `inverse` value, and a `#[cfg(test)]` module that exercises `#[sqlfunc]` null-elision rules and output type inference across infallible and fallible function signatures.
