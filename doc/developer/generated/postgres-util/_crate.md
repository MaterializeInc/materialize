---
source: src/postgres-util/src/lib.rs
revision: 45d7c63dab
---

# mz-postgres-util

PostgreSQL utility library for Materialize's Postgres sources and CDC infrastructure.
Modules are feature-gated: `replication` (WAL level checks, slot management, RLS validation), `schemas` (publication table and column introspection), and `tunnel` (connection with SSH/PrivateLink support) are compiled only when their respective features are enabled; `query` is always available.
`PostgresError` is the crate's unified error type, covering tokio_postgres errors, SSH errors, SSL errors, `UnexpectedRow`, `PublicationMissing`, and `BypassRLSRequired`.
