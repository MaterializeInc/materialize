---
source: src/storage/src/source/mysql/snapshot.rs
revision: a694e29cb3
---

# mz-storage::source::mysql::snapshot

Renders the snapshot operator for MySQL ingestion.
A designated leader worker orchestrates setup; all workers then read their assigned table partitions and emit rewind requests to the replication operator.
Handles resumption correctly by skipping tables whose outputs have already been snapshotted beyond `initial_gtid_set`.

## Snapshot setup (leader)

The leader's setup is encapsulated in `lock_and_prepare_snapshot`, which:

1. Verifies output schemas against planning-time descriptors via `verify_output_schemas`.
2. Reads row counts and, for tables with a supported single-column primary key, computes PK-range split boundaries via `sample_pk_bounds` (runs concurrently over at most `worker_count` connections). The `mysql_source_snapshot_parallelism` dyncfg disables splitting, putting every table in single-worker fallback mode. For tables whose optimizer estimate exceeds `MYSQL_SOURCE_SNAPSHOT_EXACT_COUNT_MAX_ROWS` (default 1,000,000 rows), `collect_table_statistics` reads the optimizer's row estimate from `information_schema.tables` instead of running `COUNT(*)`; for smaller tables an exact count is used. The counts feed both the PK-range sampling stride and the snapshot size gauge.
3. Acquires `LOCK TABLES … READ` and reads `@@global.gtid_executed` as the snapshot upper via `lock_tables_and_read_gtid_set`. This helper optionally sets `@@session.lock_wait_timeout` before locking.
4. Broadcasts the resulting `SnapshotInfo` (GTID set, per-table PK boundaries, and any schema-errored outputs) to all workers via a timely feedback loop.

## Snapshot reading (all workers)

Each worker starts a `REPEATABLE READ` / `CONSISTENT SNAPSHOT` transaction after receiving the broadcast, then calls `plan_worker_reads` to determine which tables (or PK ranges) it owns. Each worker re-verifies schemas in its transaction and validates that PK boundaries remain strictly monotonic under the table's current collation via `verify_pk_bounds_monotonic`. Workers drop their snapshot capability to signal readiness; the leader waits for all workers to signal before issuing `UNLOCK TABLES`.

Each worker runs `SELECT <columns> FROM table [WHERE pk >= lower AND pk < upper]` and emits rows at the minimum timestamp, then emits `RewindRequest`s so the replication operator can cancel updates that fall between `initial_gtid_set` and `snapshot_upper`.

Because the row count used for PK-range boundary computation can be an optimizer estimate for large tables, the partitions are approximate. An overestimate walks off the end of the index and stops with fewer boundaries, giving some workers less or no work. An underestimate leaves a larger final partition for the last worker. Both cases still correctly partition the table.

The leader publishes the full snapshot size gauge (from the counts) after unlocking, avoiding double-counting across workers.
