---
source: src/catalog/src/builtin/pg_catalog.rs
revision: c317ceee3c
---

# catalog::builtin::pg_catalog

Defines all built-in catalog objects for the `pg_catalog` SQL schema.

This module contains 114 public items: PostgreSQL-compatible type definitions (`BuiltinType`) and compatibility views (`BuiltinView`).

**Types** — `BuiltinType<NameReference>` constants cover the full set of PostgreSQL-compatible base types and their array counterparts. Materialize-specific extensions include `TYPE_UINT2`, `TYPE_UINT4`, `TYPE_UINT8`, `TYPE_MZ_TIMESTAMP`, `TYPE_MZ_ACL_ITEM`, and their array forms. All types use static OIDs matching the PostgreSQL type catalog to ensure wire-protocol compatibility. Every type's `CatalogTypePgMetadata` carries a `typsend_oid` field alongside `typinput_oid` and `typreceive_oid`; pseudo-types without a send function use OID 0, and generic array types use OID 2401.

**Views** — `BuiltinView` statics implement the `pg_catalog` compatibility layer: `pg_attribute`, `pg_class`, `pg_database`, `pg_depend`, `pg_description`, `pg_enum`, `pg_index`, `pg_indexes`, `pg_locks`, `pg_namespace`, `pg_proc`, `pg_range`, `pg_roles`, `pg_settings`, `pg_tables`, `pg_type`, `pg_user`, `pg_views`, and others. These views query `mz_catalog` and `mz_internal` system tables and present the results in the column layout expected by PostgreSQL clients. The `pg_user` view always returns `'********'` for the `passwd` column, redacting the actual credential value. `PG_TYPE` exposes a `typsend` column (`regproc`, not nullable) and casts `typreceive` to `regproc`; both are sourced from `mz_internal.pg_type_all_databases`. The underlying `pg_type_all_databases` keeps `typreceive` as `oid` to support a builtin index (resolving `regproc` to a name reads `current_database()`, which is unmaterializable), so `PG_TYPE` casts it to `regproc` at query time.

Each `BuiltinView` carries its SQL definition inline, an OID from `mz_pgrepr::oid`, an access list (typically `PUBLIC_SELECT`), and a `RelationDesc` describing its column types.

The file includes a comment warning that builtin definitions must not be deleted and that column removals or type changes in existing builtins break backwards compatibility with persisted user objects.
