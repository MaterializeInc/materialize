---
source: src/postgres-util/src/schemas.rs
revision: f3368c4db6
---

# mz-postgres-util::schemas

Provides schema introspection functions for PostgreSQL sources.
`get_schemas` queries `pg_namespace` and returns all schemas as `PostgresSchemaDesc` values.
`get_pg_major_version` reads `server_version_num` and divides by 10000 to obtain the major version, using `SELECT` (not `SHOW`) for compatibility with Aurora replication channels.
`publication_info` fetches full table descriptors for all or a subset of tables in a named publication.
It queries `pg_publication_tables` joined with `pg_class` and `pg_namespace` for table identities, then separately fetches column metadata from `pg_attribute` (excluding generated columns on PostgreSQL 12+) and key constraints from `pg_constraint` joined with `pg_index` (including `NULLS NOT DISTINCT` on PostgreSQL 15+).
Returns a `BTreeMap<Oid, PostgresTableDesc>`.
This module is compiled only when the `schemas` feature is enabled.
