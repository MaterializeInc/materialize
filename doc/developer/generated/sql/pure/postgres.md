---
source: src/sql/src/pure/postgres.rs
revision: 12fbe31d24
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types to Materialize types by producing `StorageScalarExpr`-based cast expressions (including OID-based casts), and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
Privilege and replica-identity queries are issued via `mz_postgres_util::query` with SQL literals constructed using the `sql!` macro, rather than calling `client.query` directly.
