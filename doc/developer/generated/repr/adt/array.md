---
source: src/repr/src/adt/array.rs
revision: b1c4c5bf57
---

# mz-repr::adt::array

Defines `Array` and `ArrayDimension` types representing SQL arrays, along with `InvalidArrayError` for constraint violations.
`Array` stores elements as a `Row` (using the datum encoding) paired with dimension metadata; it supports multi-dimensional arrays consistent with PostgreSQL's array semantics.
