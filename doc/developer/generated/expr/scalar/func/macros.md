---
source: src/expr/src/scalar/func/macros.rs
revision: 9c1e2767b0
---

# mz-expr::scalar::func::macros

Defines the `derive_unary!`, `derive_variadic!`, and `derive_binary!` macros that generate the `UnaryFunc`, `VariadicFunc`, and `BinaryFunc` enums and their delegating `impl` blocks.
Each macro takes a list of variant-name/inner-type pairs and emits the enum definition, an `eval` method, type-inference methods (`output_type`, `propagates_nulls`, etc.), a `Display` impl, and `From<InnerType>` conversions.
Also contains a `to_unary!` helper macro used by individual scalar func implementations to express their `inverse` value.
