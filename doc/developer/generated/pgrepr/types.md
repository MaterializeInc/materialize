---
source: src/pgrepr/src/types.rs
revision: 5f785f23fd
---

# mz-pgrepr::types

Defines the `Type` enum, which represents every PostgreSQL data type supported by Materialize, along with type modifier (typmod) encoding and decoding.

`Type` variants cover all standard PostgreSQL scalar, composite, and pseudo-types, plus Materialize-specific extensions: `UInt2`/`UInt4`/`UInt8`, `List`, `Map`, `MzTimestamp`, `MzAclItem`, and `Range`.
The `TypeConstraint` trait abstracts type modifiers (length, precision, scale) across `CharLength`, `IntervalConstraints`, `TimePrecision`, `TimestampPrecision`, and `NumericConstraints`; each implements pack/unpack of PostgreSQL's packed `typmod` integer.
`Type::from_oid_and_typmod` maps a PostgreSQL OID plus typmod to a `Type`, while `Type::oid` and `Type::typmod` provide the reverse direction for use by the wire protocol.
Static `LazyLock` values (`LIST`, `MAP`, `UINT2`, `UINT4`, `UINT8`, `MZ_TIMESTAMP`, `MZ_ACL_ITEM`, etc.) register Materialize's pseudo-types with the `postgres_types` crate so they can participate in `ToSql`/`FromSql` dispatch.
