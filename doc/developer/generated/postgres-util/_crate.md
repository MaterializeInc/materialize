---
source: src/postgres-util/src/lib.rs
revision: 90cd5b67af
---

# mz-postgres-util

PostgreSQL utility library for Materialize's Postgres sources and CDC infrastructure.
Modules are feature-gated: `replication` (WAL level checks, slot management, RLS validation), `schemas` (publication table and column introspection), and `tunnel` (connection with SSH/PrivateLink support) are compiled only when their respective features are enabled; `query` is always available.
The `query` module and `mz_ore::sql` are re-exported at the crate root, exposing `Sql`, `SqlFormatError`, and typed query helpers (`query`, `query_one`, `query_opt`, `query_prepared`, `query_one_prepared`, `query_opt_prepared`, `execute`, `execute_prepared`, `batch_execute`, `simple_query`, `simple_query_opt`).
When the `replication` feature is enabled, `fetch_max_lsn` and `get_is_in_recovery` are re-exported at the crate root alongside the other replication helpers. `fetch_max_lsn` accepts an `is_physical_standby` bool: for physical standbys it queries `pg_last_wal_replay_lsn()` (returning an error if the value is unexpectedly absent), and for primary servers it queries `pg_current_wal_lsn()` (returning an error if the function returns no value). `get_is_in_recovery` queries `pg_is_in_recovery()` and returns whether the server is acting as a physical replica.
`PostgresError` is the crate's unified error type, covering tokio_postgres errors, SSH errors, SSL errors, `UnexpectedRow`, `PublicationMissing`, and `BypassRLSRequired`.
