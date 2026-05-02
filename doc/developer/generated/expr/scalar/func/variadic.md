---
source: src/expr/src/scalar/func/variadic.rs
revision: ed294863cf
---

# mz-expr::scalar::func::variadic

Defines the `LazyVariadicFunc` and `EagerVariadicFunc` traits, and uses `derive_variadic\!` to generate the `VariadicFunc` enum.
`LazyVariadicFunc` requires `eval`, `output_type`, `propagates_nulls`, `introduces_nulls`, and optionally `could_error`, `is_monotone`, `is_associative`, and `is_infix_op`.
Implements variadic scalar functions (those taking a variable number of arguments) including `And`, `Or`, `Coalesce`, `Greatest`, `Least`, `ArrayCreate`, `ArrayFill`, `ArrayIndex`, `ArrayToString`, `ListCreate`, `ListIndex`, `RecordCreate`, `MapBuild`, `RangeCreate`, `ErrorIfNull`, `CaseLiteral`, `Concat`, `MakeTimestamp`, `RegexpMatch`, `RegexpReplace`, `RegexpSplitToArray`, `JsonbBuildArray`, `JsonbBuildObject`, `DateBin`, cryptographic digest/HMAC functions, and date/time formatting.
