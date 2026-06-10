---
source: src/pgrepr/src/value/interval.rs
revision: e757b4d11b
---

# mz-pgrepr::value::interval

Provides `Interval`, a newtype wrapper around `mz_repr::adt::interval::Interval` that implements `ToSql` and `FromSql` for the PostgreSQL binary wire format.
Serialization encodes the interval as three network-byte-order fields: microseconds (i64), days (i32), and months (i32), matching PostgreSQL's internal interval representation.
