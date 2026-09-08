---
source: src/expr/src/scalar/func/impls/case_literal.rs
revision: c6be08fe4f
---

# mz-expr::scalar::func::impls::case_literal

Provides a lookup-based evaluation of `CASE expr WHEN lit1 THEN res1 ... ELSE els END`.
`CaseLiteral` replaces chains of `If(Eq(expr, literal), result, If(...))` with a sorted `Vec<CaseLiteralEntry>` + binary-search lookup, turning O(n) evaluation into O(log n).
Each `CaseLiteralEntry` pairs a literal `StableRow` with the index of the corresponding result expression in `exprs`. `StableRow` is used because `CaseLiteral` is part of the stable LIR serialization surface, where raw `Row` bytes must not appear.
Represented as `CallVariadic { func: CaseLiteral { lookup, return_type }, exprs }` where `exprs[0]` is the input expression, `exprs[1..n]` are case result expressions, and `exprs[last]` is the fallback.
NULL inputs go straight to the fallback since SQL `NULL = x` is always falsy.
Implements `LazyVariadicFunc` with `propagates_nulls = false`, `introduces_nulls = true`, `could_error = false`, `is_monotone = false`, `is_associative = false`.
Helper methods `get` (binary-search lookup returning an expr index) and `insert` (sorted insertion, overwriting duplicates and returning the old index) are provided for building and querying the lookup table.
