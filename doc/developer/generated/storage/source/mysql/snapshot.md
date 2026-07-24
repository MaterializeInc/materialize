---
source: src/storage/src/source/mysql/snapshot.rs
revision: fe04b48e6f
---

# mz-storage::source::mysql::snapshot

Renders the snapshot operator for MySQL ingestion.
A designated leader worker orchestrates setup; all workers then read their assigned table partitions and emit rewind requests to the replication operator.
Handles resumption correctly by skipping tables whose outputs have already been snapshotted beyond `initial_gtid_set`.

## Snapshot setup (leader)

The leader's setup is encapsulated in `lock_and_prepare_snapshot`, which:

1. Verifies output schemas against planning-time descriptors via `verify_output_schemas`.
2. Samples exact row counts and, for tables with a supported single-column primary key, computes PK-range split boundaries via `sample_pk_bounds` (runs concurrently over at most `worker_count` connections). The `MYSQL_SOURCE_SNAPSHOT_PARALLELISM` dyncfg disables splitting, putting every table in single-worker fallback mode.
3. Acquires `LOCK TABLES … READ` and reads `@@global.gtid_executed` as the snapshot upper via `lock_tables_and_read_gtid_set`. This helper optionally sets `@@session.lock_wait_timeout` before locking. `SnapshotSetupError` classifies errors from this step, distinguishing definite errors (e.g. `ER_NO_SUCH_TABLE`) from transient ones.
4. Broadcasts the resulting `SnapshotInfo` (GTID set, per-table PK boundaries, and any schema-errored outputs) to all workers via a timely feedback loop.

## Snapshot reading (all workers)

Each worker starts a `REPEATABLE READ` / `CONSISTENT SNAPSHOT` transaction after receiving the broadcast, then calls `plan_worker_reads` to determine which tables (or PK ranges) it owns. Each worker re-verifies schemas in its transaction and validates that PK boundaries remain strictly monotonic under the table's current collation via `verify_pk_bounds_monotonic`. Workers drop their snapshot capability to signal readiness; the leader waits for all workers to signal before issuing `UNLOCK TABLES`.

Each worker runs `SELECT <columns> FROM table [WHERE pk >= lower AND pk < upper]` and emits rows at the minimum timestamp, then emits `RewindRequest`s so the replication operator can cancel updates that fall between `initial_gtid_set` and `snapshot_upper`.

The leader publishes the full snapshot size gauge (from the PK-sampling counts) after unlocking, avoiding double-counting across workers.
