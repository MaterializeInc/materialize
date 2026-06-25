---
source: src/repr/src/adt/numeric.rs
revision: 2bd0f58824
---

# mz-repr::adt::numeric

Wraps the `dec` crate's `OrderedDecimal<Numeric>` for use as Materialize's arbitrary-precision numeric type, implementing PostgreSQL-compatible `NUMERIC(precision, scale)` constraints and arithmetic.
`NumericMaxScale` and `NumericAgg` support aggregate-specific numeric handling.
