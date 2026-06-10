---
source: src/pgrepr/src/value/numeric.rs
revision: e757b4d11b
---

# mz-pgrepr::value::numeric

Provides `Numeric`, a newtype wrapper around `OrderedDecimal<AdtNumeric>` that implements `ToSql` and `FromSql` for PostgreSQL's binary numeric format.
Serialization and deserialization operate in base 10,000 units, following PostgreSQL's `NUMERIC` binary encoding: the value is decomposed into 16-bit digit groups with a weight, sign, and scale header.
Special values (NaN, positive infinity, negative infinity) are handled via PostgreSQL's designated sign bit patterns (`0xC000`, `0xD000`, `0xF000`).
A roundtrip test exercises a wide range of values including edge cases for zero, large integers, small fractions, and infinite values.
