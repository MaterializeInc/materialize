---
source: src/persist/src/postgres.rs
revision: 72f403f5ff
---

# persist::postgres

Implements the `Consensus` trait backed by a Postgres (or CockroachDB) table named `consensus` with columns `(shard, sequence_number, data)`.
Uses `deadpool-postgres` for connection pooling and maps Postgres `SERIALIZATION_FAILURE` errors to `Determinate` so that the caller can retry safely.
The `PostgresMode` enum (`CockroachDB` or `Postgres`) is detected at startup and selects between two query families.
A `dyncfg` flag (`PG_CONSENSUS_READ_COMMITTED`, name `persist_pg_consensus_read_committed`) controls the connection isolation level: when enabled on a vanilla Postgres backend, connections run under `READ COMMITTED` isolation; the flag must remain off on CockroachDB because the `CRDB_*` query family is only linearizable under `SERIALIZABLE`.
The `POSTGRES_*` query family used on vanilla Postgres is linearizable under both `READ COMMITTED` and `SERIALIZABLE`, and remains linearizable when some connections run under each level, because every operation is a single statement (a single-statement transaction takes its snapshot at the same instant under either isolation level). `compare_and_set` uses a CTE with a `FOR KEY SHARE` lock on the expected row. Shard initialization is a single statement (`POSTGRES_INIT_QUERY`) that inserts a `-1` marker row alongside seqno 0, guarded by `NOT EXISTS` over the shard; the marker is preserved across truncations (the truncation query restricts deletes to `sequence_number >= 0`) so that delayed first writers collide with it rather than re-inserting into a gap left by truncation.
The `CRDB_*` query family uses a single-statement INSERT with a subquery for the expected-seqno check (1-phase commit fast path) and a `NOT EXISTS` guard for initialization.
CockroachDB-specific DDL (stats collection and GC TTL configuration) is applied at startup to prevent unbounded tombstone accumulation.
