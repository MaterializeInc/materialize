---
source: src/persist/src/postgres.rs
revision: d6dff42fd6
---

# persist::postgres

Implements the `Consensus` trait backed by a Postgres (or CockroachDB) table named `consensus` with columns `(shard, sequence_number, data)`.
Uses `deadpool-postgres` for connection pooling and maps Postgres `SERIALIZATION_FAILURE` errors to `Determinate` so that the caller can retry safely.
A `dyncfg` flag (`USE_POSTGRES_TUNED_QUERIES`) selects Postgres-tuned query strategies when connecting to a vanilla Postgres backend; the flag is a no-op on CockroachDB.
When the flag is enabled on a Postgres backend, `compare_and_set` runs under `READ COMMITTED` isolation using a CTE with a `FOR KEY SHARE` lock rather than the CockroachDB-optimized single-statement INSERT, and shard initialization inserts a `-1` sentinel row alongside the first real row instead of using a `NOT EXISTS` guard; the truncation query always preserves the sentinel by restricting deletes to `sequence_number >= 0`.
CockroachDB-specific DDL (stats collection and GC TTL configuration) is applied at startup to prevent unbounded tombstone accumulation.
