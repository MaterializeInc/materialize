---
source: src/persist/src/postgres.rs
revision: 4a1aeff959
---

# persist::postgres

Implements the `Consensus` trait backed by a Postgres (or CockroachDB) table named `consensus` with columns `(shard, sequence_number, data)`.
Uses `deadpool-postgres` for connection pooling and maps Postgres `SERIALIZATION_FAILURE` errors to `Determinate` so that the caller can retry safely.
A `dyncfg` flag (`USE_POSTGRES_TUNED_QUERIES`) switches between a vanilla-Postgres query plan and a CockroachDB-optimised one that minimises lock acquisition.
CockroachDB-specific DDL (stats collection and GC TTL configuration) is applied at startup to prevent unbounded tombstone accumulation.
