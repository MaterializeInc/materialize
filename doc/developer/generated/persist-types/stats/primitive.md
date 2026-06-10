---
source: src/persist-types/src/stats/primitive.rs
revision: 5b0f392076
---

# persist-types::stats::primitive

Defines `PrimitiveStats<T>`, a pair of inclusive lower/upper bounds for a primitive column, and `PrimitiveStatsVariants`, an enum that erases the type parameter for serialization.
Provides `truncate_bytes` and `truncate_string` to lossy-truncate bounds to `TRUNCATE_LEN` (100 bytes), preserving soundness as lower or upper bounds.
`ColumnarStatsBuilder` implementations for all numeric Arrow types and for `&str`/`&[u8]` columns compute min/max from an Arrow array column.
