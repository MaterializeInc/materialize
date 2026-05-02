---
source: src/expr/src/scalar/func/macros.rs
revision: 61475c0097
---

# mz-expr::scalar::func::macros

Defines the `derive_unary\!`, `derive_binary\!`, and `derive_variadic\!` macros that generate the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums and their delegating `impl` blocks.
Each macro takes a list of variant-name/inner-type pairs and emits the enum definition (with standard derives including `MzReflect`), delegating methods (`eval`, `output_sql_type`, `output_type`, `propagates_nulls`, `introduces_nulls`, `could_error`, `is_monotone`, etc.), a `Display` impl, and `From<InnerType>` conversions for each variant.
Also contains a `to_unary\!` helper macro used by individual scalar function implementations to express their `inverse` value, and a `#[cfg(test)]` module that exercises `#[sqlfunc]` null-elision rules and output type inference across infallible and fallible function signatures.
