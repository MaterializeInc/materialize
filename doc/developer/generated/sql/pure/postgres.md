---
source: src/sql/src/pure/postgres.rs
revision: 3af9082af6
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types (including OID-based casts) to Materialize types, and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
