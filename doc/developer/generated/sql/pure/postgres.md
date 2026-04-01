---
source: src/sql/src/pure/postgres.rs
revision: 5f785f23fd
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types (including OID-based casts) to Materialize types, and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
