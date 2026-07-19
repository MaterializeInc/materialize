---
source: src/storage/src/source/mysql/snapshot.rs
revision: e440289ef4
---

# mz-storage::source::mysql::snapshot

Renders the snapshot operator for MySQL ingestion.
Each worker takes table locks on its assigned tables, reads the current GTID frontier as the snapshot upper, performs `SELECT <columns> FROM table` queries, and emits rewind requests to the replication operator.
Handles resumption correctly by skipping tables whose outputs have already been snapshotted beyond `initial_gtid_set`.

Table locking and GTID set retrieval are encapsulated in the `lock_tables_and_read_gtid_set` async helper, which optionally sets `@@session.lock_wait_timeout`, issues `LOCK TABLES`, and queries `global.gtid_executed`. Errors from that step are classified by `SnapshotSetupError`, which distinguishes definite errors (e.g. `ER_NO_SUCH_TABLE`) from transient ones so the caller can route them appropriately.
