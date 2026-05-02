---
source: src/expr/src/scalar/func/unary.rs
revision: 90a38f32be
---

# mz-expr::scalar::func::unary

Defines the `LazyUnaryFunc` and `EagerUnaryFunc` traits, and uses `derive_unary\!` to generate the `UnaryFunc` enum.
`LazyUnaryFunc` is the core trait all unary scalar functions must implement (either directly or via the blanket impl from `EagerUnaryFunc`); it requires `eval`, `output_sql_type`, `propagates_nulls`, `introduces_nulls`, `preserves_uniqueness`, `inverse`, `is_monotone`, `is_eliminable_cast`, and optionally `could_error`.
`EagerUnaryFunc` is a higher-level trait using associated `Input`/`Output` types that implement `InputDatumType`/`OutputDatumType`; null propagation, null introduction, and error properties are inferred automatically from the type signatures. A blanket `impl<T: EagerUnaryFunc> LazyUnaryFunc for T` bridges the two traits.
The `derive_unary\!` invocation includes variants for all built-in unary functions, including `ParseCatalogId`, `ParseCatalogPrivileges`, and `ParseCatalogCreateSql` for catalog JSON deserialization, and `RedactSql` for SQL redaction.
These properties drive null-propagation, type inference, and optimizer rewrites throughout the system.
