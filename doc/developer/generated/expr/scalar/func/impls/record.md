---
source: src/expr/src/scalar/func/impls/record.rs
revision: 61475c0097
---

# mz-expr::scalar::func::impls::record

Provides scalar function implementations for composite/record datums.
Key types:
- `CastRecordToString` -- converts a record to its text representation using `stringify_datum`, parameterized by the record's `SqlScalarType`.
- `CastRecord1ToRecord2` -- casts between two record types by applying per-field `cast_exprs` to each element.
- `RecordGet(usize)` -- extracts a single field from a record by positional index; output type is determined by inspecting the record's field types.
All three implement `LazyUnaryFunc` directly (not via `#[sqlfunc]`), since they carry runtime parameters.
