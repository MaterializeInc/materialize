---
source: src/expr/src/scalar/func/macros.rs
revision: d08f8f74a0
---

# mz-expr::scalar::func::macros

Defines the `derive_unary\!`, `derive_binary\!`, and `derive_variadic\!` macros that generate the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums and their delegating `impl` blocks.
Each macro takes a list of variant-name/inner-type pairs and emits the enum definition (with standard derives including `MzReflect`), delegating methods (`eval`, `output_sql_type`, `output_type`, `propagates_nulls`, `introduces_nulls`, `could_error`, `is_monotone`, etc.), a `Display` impl, and `From<InnerType>` conversions for each variant.
`derive_binary!` additionally delegates `is_infinity_monotone` to `LazyBinaryFunc::is_infinity_monotone` on each variant, making it callable as `BinaryFunc::is_infinity_monotone(&self)`.
Also contains a `to_unary\!` helper macro used by individual scalar function implementations to express their `inverse` value, and a `#[cfg(test)]` module that exercises `#[sqlfunc]` null-elision rules and output type inference across infallible and fallible function signatures.
