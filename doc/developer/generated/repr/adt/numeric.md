---
source: src/repr/src/adt/numeric.rs
revision: 8f136c6f83
---

# mz-repr::adt::numeric

Wraps the `dec` crate's `OrderedDecimal<Numeric>` for use as Materialize's arbitrary-precision numeric type, implementing PostgreSQL-compatible `NUMERIC(precision, scale)` constraints and arithmetic.
`NumericMaxScale` and `NumericAgg` support aggregate-specific numeric handling.
