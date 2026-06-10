---
source: src/catalog/src/builtin/pg_catalog.rs
revision: 106351ce78
---

# catalog::builtin::pg_catalog

Defines all built-in catalog objects for the `pg_catalog` SQL schema.

This module contains 114 public items: PostgreSQL-compatible type definitions (`BuiltinType`) and compatibility views (`BuiltinView`).

**Types** — `BuiltinType<NameReference>` constants cover the full set of PostgreSQL-compatible base types and their array counterparts. Materialize-specific extensions include `TYPE_UINT2`, `TYPE_UINT4`, `TYPE_UINT8`, `TYPE_MZ_TIMESTAMP`, `TYPE_MZ_ACL_ITEM`, and their array forms. All types use static OIDs matching the PostgreSQL type catalog to ensure wire-protocol compatibility.

**Views** — `BuiltinView` statics implement the `pg_catalog` compatibility layer: `pg_attribute`, `pg_class`, `pg_database`, `pg_depend`, `pg_description`, `pg_enum`, `pg_index`, `pg_indexes`, `pg_locks`, `pg_namespace`, `pg_proc`, `pg_range`, `pg_roles`, `pg_settings`, `pg_tables`, `pg_type`, `pg_user`, `pg_views`, and others. These views query `mz_catalog` and `mz_internal` system tables and present the results in the column layout expected by PostgreSQL clients. The `pg_user` view always returns `'********'` for the `passwd` column, redacting the actual credential value.

Each `BuiltinView` carries its SQL definition inline, an OID from `mz_pgrepr::oid`, an access list (typically `PUBLIC_SELECT`), and a `RelationDesc` describing its column types.

The file includes a comment warning that builtin definitions must not be deleted and that column removals or type changes in existing builtins break backwards compatibility with persisted user objects.
