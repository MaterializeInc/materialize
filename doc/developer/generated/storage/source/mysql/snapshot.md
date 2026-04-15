---
source: src/storage/src/source/mysql/snapshot.rs
revision: b0fa98e931
---

# mz-storage::source::mysql::snapshot

Renders the snapshot operator for MySQL ingestion.
Each worker takes table locks on its assigned tables, reads the current GTID frontier as the snapshot upper, performs `SELECT * FROM table` queries, and emits rewind requests to the replication operator.
Handles resumption correctly by skipping tables whose outputs have already been snapshotted beyond `initial_gtid_set`.
