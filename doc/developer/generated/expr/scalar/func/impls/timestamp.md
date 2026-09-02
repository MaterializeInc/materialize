---
source: src/expr/src/scalar/func/impls/timestamp.rs
revision: 38293e9a93
---

# mz-expr::scalar::func::impls::timestamp

Provides scalar function implementations for `timestamp` and `timestamptz` datums: casts to/from string, date, time, and between the two variants; precision adjustment (`AdjustTimestampPrecision`, `AdjustTimestampTzPrecision`); `to_char` formatting (`ToCharTimestamp`, `ToCharTimestampTz`); interval arithmetic; `date_trunc`; time-zone conversions; and date-part extraction.
