---
source: src/repr/src/lib.rs
revision: 2982634c0d
---

# mz-repr

The lingua franca of Materialize: defines the core data types that all layers of the stack agree on at their boundaries.

## Module structure

* `scalar` — `Datum` (value enum), `DatumKind`, and a dual-type system: `SqlScalarType` (SQL-level, with modifiers like `VarChar`, `Char`, `Oid`) and `ReprScalarType` (repr-level, collapsed variants); `SqlScalarBaseType`/`ReprScalarBaseType` enum-kind tags; `SqlContainerType` trait; proptest strategies
* `relation` — `RelationDesc`, `ColumnName`, and a matching dual-type split: `SqlColumnType`/`SqlRelationType` (SQL-level) and `ReprColumnType`/`ReprRelationType` (repr-level); schema evolution (`RelationDescDiff`, `VersionedRelationDesc`); `backport_nullability` for reconciling nullability across the two type layers
* `relation_and_scalar` — shared protobuf definitions bridging the `relation` and `scalar` modules
* `row` — `Row`, `RowPacker`, `RowRef`, `RowArena`, `DatumList<'a, T>` (generic over `T`, defaulting to `Datum<'a>`), `DatumMap`, `SharedRow`; Arrow columnar encoding (`encode`); abstract iteration (`iter`)
* `update` — `Rows` (a compact contiguous-byte sequence of encoded rows), `RowsBuilder`, `SharedSlice`, `UpdateCollection`, and `UpdateCollectionBuilder`; provides the low-level storage substrate used by sorted row collections
* `adt` — PostgreSQL-compatible ADTs: arrays, char, date, datetime, interval, JSONB, ACL items, numeric, range, regex, system OIDs, timestamps, varchar
* `timestamp` — `Timestamp` (system-wide `u64` millisecond type implementing Timely/differential traits)
* `diff` — `Diff` type alias (`Overflowing<i64>`)
* `strconv` — string-to-value conversion matching PostgreSQL text format
* `explain` — `Explain` trait and text/JSON/DOT/tracing format implementations
* `stats` — persist pushdown statistics for complex types
* `global_id` / `catalog_item_id` / `role_id` / `network_policy_id` — identifier types
* `namespaces` — well-known schema name constants
* `optimize` — `OptimizerFeatures`, `OptimizerFeatureOverrides`, and `OverrideFrom` trait
* `refresh_schedule` — REFRESH EVERY/AT schedule representation
* `fixed_length` — `ToDatumIter` abstraction
* `datum_vec` — reusable `Datum` scratch buffer
* `bytes` — PostgreSQL-compatible `ByteSize`
* `user` — external user auth metadata

## Key dependencies

`mz-ore`, `mz-persist-types` (columnar codec, stats), `mz-proto` (protobuf), `mz-lowertest`, `mz-pgtz`, `mz-sql-parser`, `timely`, `differential-dataflow`, `dec` (numeric), `chrono`, `arrow`.

## Downstream consumers

Used by virtually every other crate in Materialize: `mz-expr`, `mz-compute`, `mz-storage-*`, `mz-adapter`, `mz-catalog`, `mz-sql`, `mz-pgrepr`, `mz-interchange`, and more.
