---
source: src/storage-types/src/sources/casts.rs
revision: f2a5b6012b
---

# storage-types::sources::casts

Defines storage-specific scalar expression and cast function types, decoupled from `mz_expr::MirScalarExpr` to ensure stable, version-independent evaluation behavior across storage releases.

`StorageScalarExpr` is a simplified scalar expression covering only the subset of operations needed for string-to-column casts: column references (`Column`), literals (`Literal`), unary function applications (`CallUnary`), and null-error guards (`ErrorIfNull`).

`CastFunc` is an enum mirroring the subset of `mz_expr::UnaryFunc` variants used when casting source columns, covering casts from `String` to all SQL scalar types (Bool, integer and unsigned integer widths, floats, Date, Time, Timestamp, TimestampTz, Interval, Uuid, Bytes, Jsonb, MzTimestamp, Numeric, Char, VarChar, Array, List, Map, Range, Int2Vector, PgLegacyChar, PgLegacyName, Oid).
There are two Oid cast variants: `CastStringToOid` calls `strconv::parse_oid_legacy` (accepting only the `i32` range) and is used by source exports created before the OID cast was widened, preserving evaluation stability for persisted expressions; `CastStringToOidFullRange` calls `strconv::parse_oid` (accepting the full `u32` range) and is used by exports created after the widening. This split satisfies the stability contract: replication re-casts old tuples on delete, so changing the persisted cast for existing exports would break retraction symmetry.

The `eval` implementations delegate to `mz_repr::strconv::parse_*` functions. Changes to error variants, error messages, or output types are breaking changes for storage and require a migration.
