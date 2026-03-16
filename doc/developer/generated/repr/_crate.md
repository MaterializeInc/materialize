---
source: src/repr/src/lib.rs
revision: 85fd84fde9
---

# mz-repr

The lingua franca of Materialize: defines the core data types that all layers of the stack agree on at their boundaries.

## Module structure

* `scalar` — `Datum` (value enum), `ScalarType`, `DatumKind`, and proptest strategies
* `relation` — `RelationDesc`, `ColumnName`, `ColumnType`, schema evolution (`RelationDescDiff`, `VersionedRelationDesc`)
* `row` — `Row`, `RowPacker`, `RowRef`, `RowArena`, `DatumList`, `DatumMap`, `SharedRow`; Arrow columnar encoding (`encode`); abstract iteration (`iter`)
* `adt` — PostgreSQL-compatible ADTs: arrays, char, date, datetime, interval, JSONB, ACL items, numeric, range, regex, system OIDs, timestamps, varchar
* `timestamp` — `Timestamp` (system-wide `u64` millisecond type implementing Timely/differential traits)
* `diff` — `Diff` type alias (`Overflowing<i64>`)
* `strconv` — string ↔ value conversion matching PostgreSQL text format
* `explain` — `Explain` trait and text/JSON/DOT/tracing format implementations
* `stats` — persist pushdown statistics for complex types
* `global_id` / `catalog_item_id` / `role_id` / `network_policy_id` — identifier types
* `namespaces` — well-known schema name constants
* `optimize` — optimizer feature flags
* `refresh_schedule` — REFRESH EVERY/AT schedule representation
* `fixed_length` — `ToDatumIter` abstraction
* `datum_vec` — reusable `Datum` scratch buffer
* `bytes` — PostgreSQL-compatible `ByteSize`
* `user` — external user auth metadata

## Key dependencies

`mz-ore`, `mz-persist-types` (columnar codec, stats), `mz-proto` (protobuf), `mz-lowertest`, `mz-pgtz`, `mz-sql-parser`, `timely`, `differential-dataflow`, `dec` (numeric), `chrono`, `arrow`.

## Downstream consumers

Used by virtually every other crate in Materialize: `mz-expr`, `mz-compute`, `mz-storage-*`, `mz-adapter`, `mz-catalog`, `mz-sql`, `mz-pgrepr`, `mz-interchange`, and more.
