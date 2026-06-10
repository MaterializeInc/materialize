---
source: src/pgrepr/src/value/unsigned.rs
revision: e757b4d11b
---

# mz-pgrepr::value::unsigned

Defines `UInt2`, `UInt4`, and `UInt8` — newtype wrappers for Materialize's non-standard unsigned integer types — with `ToSql`/`FromSql` implementations for the PostgreSQL binary format.
Each type is serialized as a big-endian integer and accepted only for its corresponding Materialize-specific OID (from the `oid` module), since these types do not exist in standard PostgreSQL.
