---
source: src/repr/src/scalar.rs
revision: 5f785f23fd
---

# mz-repr::scalar

Defines `Datum<'a>`, the core value enum covering all SQL types (null, booleans, integers, floats, strings, bytes, dates, times, intervals, UUIDs, arrays, lists, maps, ranges, JSONB, and more), along with `DatumKind` (a copy-able type tag derived via `EnumKind`).

Introduces a dual-type system for scalar types:

* **`SqlScalarType`**: SQL-level type enum preserving modifiers and distinct variants for `VarChar`, `Char`, `PgLegacyChar`, `PgLegacyName`, `Oid`, `RegClass`, `RegProc`, `RegType`, etc. Derived `SqlScalarBaseType` provides a copy-able enum-kind tag.
* **`ReprScalarType`**: repr-level type enum with collapsed variants (e.g., `String` covers `VarChar`/`Char`/`PgLegacyName`; `UInt32` covers `Oid`/`RegClass`/`RegProc`/`RegType`; `UInt8` covers `PgLegacyChar`). Derived `ReprScalarBaseType` provides a copy-able enum-kind tag. Used in compute and storage layers where modifier distinctions are irrelevant.

`SqlContainerType` is a trait implemented by container datum types (`Array`, `Range`) to provide compile-time element-type unwrap/wrap on `SqlScalarType`, used by the `#[sqlfunc]` proc macro.

`AsColumnType`, `InputDatumType`, and `OutputDatumType` traits bridge between native Rust types and their SQL column type representations.
`Int2Vector` provides PostgreSQL `int2vector` compatibility; `ExcludeNull`, `OptionalArg`, and `Variadic` are markers for scalar function signatures.
Proptest strategies (`arb_datum`, `arb_datum_for_scalar`, `arb_datum_for_column`, etc.) support property-based testing.
