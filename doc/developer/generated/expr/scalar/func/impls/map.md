---
source: src/expr/src/scalar/func/impls/map.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::map

Provides scalar function implementations for Materialize map datums.
Key types:
- `CastMapToString` -- converts a map to its text representation via `stringify_datum`, parameterized by `SqlScalarType`.
- `MapLength` -- returns the number of entries in a map as `i32` (generated via `#[sqlfunc]`).
- `MapBuildFromRecordList` -- constructs a map from a list of key-value record pairs, parameterized by `value_type`.
All manual structs implement `LazyUnaryFunc` directly.
