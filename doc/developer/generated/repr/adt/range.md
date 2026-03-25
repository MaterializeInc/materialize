---
source: src/repr/src/adt/range.rs
revision: ccd84bf8e8
---

# mz-repr::adt::range

Defines `Range<D>`, a generic SQL range type with inclusive/exclusive lower and upper bounds, matching PostgreSQL range semantics.
`RangeLowerBound` and `RangeUpperBound` represent the optional finite or infinite endpoints; `InvalidRangeError` covers constraint violations.
