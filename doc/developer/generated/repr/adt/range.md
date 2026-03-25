---
source: src/repr/src/adt/range.rs
revision: fe91a762d1
---

# mz-repr::adt::range

Defines `Range<D>`, a generic SQL range type with inclusive/exclusive lower and upper bounds, matching PostgreSQL range semantics.
`RangeLowerBound` and `RangeUpperBound` represent the optional finite or infinite endpoints; `InvalidRangeError` covers constraint violations.
`Range<Datum<'_>>` implements the `SqlContainerType` trait, providing compile-time unwrap/wrap of `SqlScalarType::Range` element types for use with the `#[sqlfunc]` proc macro.
