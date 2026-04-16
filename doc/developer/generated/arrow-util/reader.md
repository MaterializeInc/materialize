---
source: src/arrow-util/src/reader.rs
revision: 3fbe777cf7
---

# reader

Decodes Arrow `StructArray` columns into Materialize `Row`s, serving as the inverse of `ArrowBuilder`.
`ArrowReader` performs a one-time downcast of each column array into a typed `ColReader` enum variant at construction time, avoiding repeated dynamic dispatch on every row read.
It validates that the provided `RelationDesc` and `StructArray` have matching column counts, names, and compatible types.
The reader supports a wider set of Arrow types than the builder produces (e.g., `Date64`, `Time32`, `Float16`, `Decimal256`, `BinaryView`) to allow reading Parquet files written by other tools, including `Map` columns (string-keyed maps, whose keys are sorted on read to satisfy `Datum::Map`'s ordering invariant) and `Interval` columns (year-month, day-time, and month-day-nano representations).
