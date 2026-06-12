---
source: src/postgres-util/src/lib.rs
revision: 12fbe31d24
---

# mz-postgres-util

PostgreSQL utility library for Materialize's Postgres sources and CDC infrastructure.
Modules are feature-gated: `replication` (WAL level checks, slot management, RLS validation), `schemas` (publication table and column introspection), and `tunnel` (connection with SSH/PrivateLink support) are compiled only when their respective features are enabled; `query` is always available.
The `query` module and `mz_ore::sql` are re-exported at the crate root, exposing `Sql`, `SqlFormatError`, and typed query helpers (`query`, `query_one`, `query_opt`, `query_prepared`, `query_one_prepared`, `query_opt_prepared`, `execute`, `execute_prepared`, `batch_execute`, `simple_query`, `simple_query_opt`).
`PostgresError` is the crate's unified error type, covering tokio_postgres errors, SSH errors, SSL errors, `UnexpectedRow`, `PublicationMissing`, and `BypassRLSRequired`.
