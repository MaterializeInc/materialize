---
source: src/expr/src/scalar/func/variadic.rs
revision: d01594110c
---

# mz-expr::scalar::func::variadic

Defines the `LazyVariadicFunc` and `EagerVariadicFunc` traits, and uses `derive_variadic\!` to generate the `VariadicFunc` enum.
`LazyVariadicFunc` requires `eval`, `output_type`, `propagates_nulls`, `introduces_nulls`, and optionally `could_error`, `is_monotone`, `is_associative`, and `is_infix_op`.
Implements variadic scalar functions (those taking a variable number of arguments) including `And`, `Or`, `Coalesce`, `Greatest`, `Least`, `ArrayCreate`, `ArrayFill`, `ArrayIndex`, `ArrayToString`, `ListCreate`, `ListIndex`, `RecordCreate`, `MapBuild`, `RangeCreate`, `ErrorIfNull`, `CaseLiteral`, `Concat`, `MakeTimestamp`, `RegexpMatch`, `RegexpReplace`, `RegexpSplitToArray`, `JsonbBuildArray`, `JsonbBuildObject`, `DateBin`, cryptographic digest/HMAC functions, and date/time formatting.
`array_position` searches a one-dimensional array for the first element equal to the search term, returning its 1-based position or `NULL` if not found. Comparisons use IS NOT DISTINCT FROM semantics: a `NULL` search term matches a `NULL` element, because `Datum` equality already treats `Null == Null` as true. A `NULL` initial-position argument is rejected with an error before the search begins. This diverges from PostgreSQL in one corner: PostgreSQL validates the initial position only after a fast path that can return early when the search term is `NULL` and the array contains no `NULL` element, so the error is conditional on array contents; that complexity is not reproduced here.
