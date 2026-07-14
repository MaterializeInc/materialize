---
source: src/persist/src/postgres.rs
revision: 83c55157ed
---

# persist::postgres

Implements the `Consensus` trait backed by a Postgres (or CockroachDB) table named `consensus` with columns `(shard, sequence_number, data)`.
Uses `deadpool-postgres` for connection pooling and maps Postgres `SERIALIZATION_FAILURE` errors to `Determinate` so that the caller can retry safely.
The `PostgresMode` enum (`CockroachDB` or `Postgres`) is detected at startup and selects between two query families.
A `dyncfg` flag (`PG_CONSENSUS_READ_COMMITTED`, name `persist_pg_consensus_read_committed`) controls the connection isolation level: when enabled on a vanilla Postgres backend, connections run under `READ COMMITTED` isolation; the flag must remain off on CockroachDB because the `CRDB_*` query family is only linearizable under `SERIALIZABLE`.
The `POSTGRES_*` query family used on vanilla Postgres is designed to be linearizable under `READ COMMITTED`. `compare_and_set` uses a CTE with a `FOR KEY SHARE` lock on the expected row. Shard initialization inserts a `-1` sentinel row alongside seqno 0 in a transaction and commits only if `max(sequence_number)` is 0; this sentinel is preserved across truncations (the truncation query restricts deletes to `sequence_number >= 0`) to remain safe during rolling upgrades alongside older versions.
The `CRDB_*` query family uses a single-statement INSERT with a subquery for the expected-seqno check (1-phase commit fast path) and a `NOT EXISTS` guard for initialization.
CockroachDB-specific DDL (stats collection and GC TTL configuration) is applied at startup to prevent unbounded tombstone accumulation.
