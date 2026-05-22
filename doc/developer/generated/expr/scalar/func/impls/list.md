---
source: src/expr/src/scalar/func/impls/list.rs
revision: 3e0299fb00
---

# mz-expr::scalar::func::impls::list

Provides scalar function implementations for Materialize list datums.
Key types:
- `CastListToString` -- text representation via `stringify_datum`, parameterized by `SqlScalarType`.
- `CastListToJsonb` -- converts list elements to JSONB using a per-element `cast_element` expression.
- `CastList1ToList2` -- element-wise cast between two list types using `cast_expr` and `return_ty`.
- `ListLength` -- returns the number of elements as `i32` (via `#[sqlfunc]`).
- `ListLengthMax` -- binary function that returns the maximum length at a given nesting layer, implementing `EagerBinaryFunc` directly with a `max_layer` parameter. The recursive helper `max_len_on_layer` iterates all siblings at each layer and skips non-list datums (e.g. NULLs), so deeper lengths of non-NULL elements after a NULL sibling are correctly observed.
