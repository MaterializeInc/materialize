---
source: src/repr/src/scalar.rs
revision: 9c1e2767b0
---

# mz-repr::scalar

Defines `Datum<'a>`, the core value enum covering all SQL types (null, booleans, integers, floats, strings, bytes, dates, times, intervals, UUIDs, arrays, lists, maps, ranges, JSONB, and more), along with `ScalarType` (the type-level counterpart) and `DatumKind` (a copy-able type tag).
`AsColumnType` and `OutputDatumType`/`InputDatumType` traits bridge between Rust native types and their column type representations; proptest `Arbitrary` strategies (`arb_datum`, `arb_datum_for_scalar`, etc.) support property-based testing.
Provides `Int2Vector` for PostgreSQL `int2vector` compatibility and `ExcludeNull`/`OptionalArg`/`Variadic` markers for scalar function signatures.
