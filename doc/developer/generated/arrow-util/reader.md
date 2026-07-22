---
source: src/arrow-util/src/reader.rs
revision: 89b4fd6a04
---

# reader

Decodes Arrow `StructArray` columns into Materialize `Row`s, serving as the inverse of `ArrowBuilder`.
`ArrowReader` performs a one-time downcast of each column array into a typed `ColReader` enum variant at construction time, avoiding repeated dynamic dispatch on every row read.
It validates that the provided `RelationDesc` and `StructArray` have matching column counts, names, and compatible types.
The reader supports a wider set of Arrow types than the builder produces (e.g., `Date64`, `Time32`, `Time64`, `Float16`, `Decimal256`, `BinaryView`) to allow reading Parquet files written by other tools, including `Oid` columns (decoded identically to `UInt32`), `MzTimestamp` columns (decoded from `UInt64`), `TimestampTz` columns (microsecond-precision timezone-aware timestamps; the timezone string is ignored since the array stores UTC-normalized instants), `Char` and `VarChar` columns (decoded as strings, matching the `String` path), `Map` columns (string-keyed maps, whose keys are sorted on read to satisfy `Datum::Map`'s ordering invariant), `Interval` columns (year-month, day-time, and month-day-nano representations), `Range` columns (five-field structs with `lower`, `upper`, `lower_inclusive`, `upper_inclusive`, and `empty` fields; discrete ranges from external writers are canonicalized via `push_range` so they compare and hash equal to internally-constructed values), `Array` columns (encoded as a two-field struct with an `items` `List` and a `dimensions` count; the `dimensions` field is accepted as either `UInt8` as written by the builder or `Int32` as widened by an Iceberg round-trip; only 0- and 1-dimensional arrays can be decoded since per-dimension extents are not stored), and `LargeList` columns (parallel to `List` but with 64-bit offsets).
`ColReader::Decimal128` and `ColReader::Decimal256` carry a `destination_max_scale` field derived from the target column's `SqlScalarType::Numeric { max_scale }`. When present, each decoded value is rescaled via `rescale` to that scale before it is stored as a `Datum::Numeric`, ensuring that Parquet-sourced numerics respect the declared column precision.
