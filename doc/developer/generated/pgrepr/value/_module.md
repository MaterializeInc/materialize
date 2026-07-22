---
source: src/pgrepr/src/value.rs
revision: f147b1341a
---

# mz-pgrepr::value

Defines the `Value` enum — the central PostgreSQL datum type — together with its serialization to and from both the PostgreSQL text and binary wire formats, and bidirectional conversion to and from `mz_repr::Datum`.

`Value` mirrors every PostgreSQL type supported by Materialize, including Materialize-specific extensions (unsigned integers, `MzTimestamp`, lists, maps, ranges, and ACL items).
The main entry points are `Value::from_datum` (converting an `mz_repr::Datum` to a `Value`), `Value::into_datum` (the reverse), `Value::encode` (writing to the wire), and `Value::decode` (reading from the wire). When decoding a `Numeric` value, the internal `rescale_numeric` helper rescales the decoded value to the `max_scale` declared by the target `Type::Numeric` constraints, if any, so that text, CSV, and binary decoding all produce consistently scaled results.
The helper function `values_from_row` converts a full `mz_repr::RowRef` into a `Vec<Option<Value>>` for use by the pgwire protocol layer.

Both text-format and binary-format decoding reject NUL (0x00) characters, matching PostgreSQL behavior. In the text path, `reject_nul(s)` is called once on the raw UTF-8 string before dispatching to any type-specific parser. In the binary path, `Text`, `BpChar`, and `VarChar` values go through `decode_binary_string`, which decodes the binary string and then calls `reject_nul`; `Name` values call `reject_nul` directly after decoding. Both helpers return `NulCharacterError` (from the `error` module) on failure.

The child modules supply `ToSql`/`FromSql` implementations for types that require non-trivial encoding logic:

* `error` — `NulCharacterError` and `IntoDatumError` for decoding and datum conversion errors.
* `interval` — PostgreSQL binary encoding for intervals.
* `jsonb` — PostgreSQL binary JSONB encoding.
* `numeric` — PostgreSQL base-10,000 binary encoding for arbitrary-precision numerics.
* `record` — `FromSql` for composite (record) types via a macro-generated tuple impl.
* `unsigned` — binary encoding for the Materialize-specific `uint2`/`uint4`/`uint8` types.
