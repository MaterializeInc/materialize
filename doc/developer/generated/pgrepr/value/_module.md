---
source: src/pgrepr/src/value.rs
revision: 59358e137b
---

# mz-pgrepr::value

Defines the `Value` enum — the central PostgreSQL datum type — together with its serialization to and from both the PostgreSQL text and binary wire formats, and bidirectional conversion to and from `mz_repr::Datum`.

`Value` mirrors every PostgreSQL type supported by Materialize, including Materialize-specific extensions (unsigned integers, `MzTimestamp`, lists, maps, ranges, and ACL items).
The main entry points are `Value::from_datum` (converting an `mz_repr::Datum` to a `Value`), `Value::into_datum` (the reverse), `Value::encode` (writing to the wire), and `Value::decode` (reading from the wire).
The helper function `values_from_row` converts a full `mz_repr::RowRef` into a `Vec<Option<Value>>` for use by the pgwire protocol layer.

The child modules supply `ToSql`/`FromSql` implementations for types that require non-trivial encoding logic:

* `error` — `IntoDatumError` for fallible datum conversion.
* `interval` — PostgreSQL binary encoding for intervals.
* `jsonb` — PostgreSQL binary JSONB encoding.
* `numeric` — PostgreSQL base-10,000 binary encoding for arbitrary-precision numerics.
* `record` — `FromSql` for composite (record) types via a macro-generated tuple impl.
* `unsigned` — binary encoding for the Materialize-specific `uint2`/`uint4`/`uint8` types.
