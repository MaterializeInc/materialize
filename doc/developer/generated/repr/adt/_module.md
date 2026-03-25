---
source: src/repr/src/adt.rs
revision: dc6f8a7d55
---

# mz-repr::adt

Groups all abstract data type implementations for Materialize's SQL type system, mirroring PostgreSQL ADTs where possible.
Covers arrays, characters, dates, datetimes, intervals, JSONB, ACL items, numerics, ranges, regexes, system OID types, timestamps, and varchar — each matching the PostgreSQL semantics for the corresponding type.
These types are used directly as variants of `Datum` and `ScalarType` in the rest of the codebase.
