---
source: src/expr/src/scalar/func/impls/range.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::range

Provides scalar function implementations for PostgreSQL range types.
Key types:
- `CastRangeToString` -- converts a range to its text representation via `stringify_datum`, parameterized by `SqlScalarType`, implementing `LazyUnaryFunc` directly.
- `RangeLower` / `RangeUpper` -- extract the lower/upper bound from a range (via `#[sqlfunc]`); `RangeLower` is marked `is_monotone = true`.
- `RangeEmpty` -- returns whether the range is empty.
- `RangeLowerInc` / `RangeUpperInc` -- return whether the lower/upper bound is inclusive.
- `RangeLowerInf` / `RangeUpperInf` -- return whether the lower/upper bound is unbounded.
All `#[sqlfunc]` functions are generic over the range element type `T`.
