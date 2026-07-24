---
source: src/expr/src/scalar/func/impls/timestamp.rs
revision: 26cb0194dc
---

# mz-expr::scalar::func::impls::timestamp

Provides scalar function implementations for `timestamp` and `timestamptz` datums: casts to/from string, date, time, numeric epoch, and between the two variants; interval arithmetic; `date_trunc`; time-zone conversions; and date-part extraction.
