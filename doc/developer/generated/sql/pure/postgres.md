---
source: src/sql/src/pure/postgres.rs
revision: f2a5b6012b
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types to Materialize types by producing `StorageScalarExpr`-based cast expressions (including OID-based casts), and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
Privilege and replica-identity queries are issued via `mz_postgres_util::query` with SQL literals constructed using the `sql!` macro, rather than calling `client.query` directly.
When generating subsource statements, table constraints (unique keys) are omitted for any key whose columns are not all present in the subsource — for example when some columns are excluded via `EXCLUDE COLUMNS`. Only keys where every referenced column is included are propagated.
`generate_column_casts` now takes a `cast_oid_full_range: bool` parameter that is passed through to `pg_type_to_cast_func`; newly purified exports always set `cast_oid_full_range: true` in their `SourceExportStatementDetails::Postgres`.
