---
source: src/sql/src/pure/postgres.rs
revision: 4229f00347
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types to Materialize types by producing `StorageScalarExpr`-based cast expressions (including OID-based casts), and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
Privilege and replica-identity queries are issued via `mz_postgres_util::query` with SQL literals constructed using the `sql!` macro, rather than calling `client.query` directly.
When generating subsource statements, table constraints (unique keys) are omitted for any key whose columns are not all present in the subsource — for example when some columns are excluded via `EXCLUDE COLUMNS`. Only keys where every referenced column is included are propagated.
