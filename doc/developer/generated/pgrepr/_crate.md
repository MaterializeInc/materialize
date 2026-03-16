---
source: src/pgrepr/src/lib.rs
revision: ccd84bf8e8
---

# mz-pgrepr

Provides representation and serialization of PostgreSQL data types for use in Materialize's pgwire layer, bridging `mz_repr::Datum` values and the PostgreSQL binary/text wire formats.

## Module structure

* `oid` — re-exports OID constants from `mz-pgrepr-consts`.
* `types` — the `Type` enum mapping every supported PostgreSQL type (including Materialize extensions) to OIDs and type modifiers.
* `value` — the `Value` enum, `Datum`↔`Value` conversion, and wire-format encoding/decoding; child modules handle complex types (numeric, interval, jsonb, record, unsigned integers).

## Key types

* `Value` — a PostgreSQL datum; supports text and binary encoding via `mz_pgwire_common::Format`.
* `Type` — the type of a `Value`, with typmod encoding/decoding.
* `values_from_row` — converts an `mz_repr::RowRef` to a vector of `Option<Value>` for pgwire responses.

## Dependencies

Direct dependencies include `mz-repr` (for `Datum` and ADT types), `mz-pgwire-common` (for `Format`), `mz-pgrepr-consts` (for OIDs), `postgres-types` (for `ToSql`/`FromSql`), `bytes`, `byteorder`, `chrono`, `dec`, and `uuid`.

## Downstream consumers

Used by `mz-pgwire` (query result encoding), `mz-adapter` (parameter binding and result formatting), and any component that serializes Materialize data for PostgreSQL clients.
