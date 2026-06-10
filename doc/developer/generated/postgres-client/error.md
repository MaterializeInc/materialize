---
source: src/postgres-client/src/error.rs
revision: 94acc9077d
---

# mz-postgres-client::error

Defines `PostgresError`, an enum distinguishing `Determinate` (e.g., serialization failures) from `Indeterminate` errors returned by Postgres or the connection pool.
Provides `From` conversions from `tokio_postgres::Error`, `deadpool_postgres::PoolError`, `anyhow::Error`, and `tokio::task::JoinError`, classifying each as determinate or indeterminate based on the SQL error code.
