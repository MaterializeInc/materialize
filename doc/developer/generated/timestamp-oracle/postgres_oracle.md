---
source: src/timestamp-oracle/src/postgres_oracle.rs
revision: 12fbe31d24
---

# mz-timestamp-oracle::postgres_oracle

Implements `PostgresTimestampOracle<N>`, the primary production oracle backed by Postgres or CockroachDB.
Each timeline maps to a row in a single `timestamp_oracle` table; `write_ts` and `apply_write` use `UPDATE … RETURNING` with optimistic retries, while `read_ts` issues a `SELECT`.
`PostgresTimestampOracleConfig` holds connection pool settings; `DynamicConfig` and `TimestampOracleParameters` allow live-updating retry and timeout parameters via LD flags.
`retry_fallible` is the central retry loop that wraps database calls, records metrics, and re-establishes connections on transient errors.
Database calls are issued through module-private wrapper functions (`pg_batch_execute`, `pg_query_one_prepared`, `pg_execute_prepared`, `pg_txn_query_prepared`, `pg_txn_query_one_prepared`) that keep the `clippy::disallowed_methods` suppressions localized; this module does not use `mz-postgres-util` wrappers because it keeps its Postgres surface local.
