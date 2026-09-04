---
source: src/expr/src/scalar/func/impls/range.rs
revision: 95baa04a85
---

# mz-expr::scalar::func::impls::range

Provides scalar function implementations for PostgreSQL range types.
Key types:
- `CastRangeToString` -- converts a range to its text representation via `stringify_datum`, parameterized by `SqlScalarType`, implementing `LazyUnaryFunc` directly.
- `RangeLower` / `RangeUpper` -- extract the lower/upper bound from a range (via `#[sqlfunc]`); `RangeLower` is marked `is_monotone = true`. The monotone claim survives the function mapping empty and unbounded-lower ranges to NULL because those NULL-producing inputs form a downward-closed prefix of the range ordering (`None` inner sorts below `Some`, and a `None` lower bound sorts below every finite one), so a range whose endpoints both yield values contains no NULL-yielding interior. `RangeUpper` is not marked monotone.
- `RangeEmpty` -- returns whether the range is empty.
- `RangeLowerInc` / `RangeUpperInc` -- return whether the lower/upper bound is inclusive.
- `RangeLowerInf` / `RangeUpperInf` -- return whether the lower/upper bound is unbounded.
All `#[sqlfunc]` functions are generic over the range element type `T`.
