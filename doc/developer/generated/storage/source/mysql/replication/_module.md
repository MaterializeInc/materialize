---
source: src/storage/src/source/mysql/replication.rs
revision: e79a6d96d9
---

# mz-storage::source::mysql::replication

Renders the replication operator for MySQL ingestion on a single worker, consuming the MySQL binlog stream from a GTID-based resume position.
Manages GTID partition tracking (`GtidReplicationPartitions`), processes rewind requests from the snapshot operator, delegates event decoding to `events.rs` and `context.rs`, and detects incompatible schema changes.
The replication frontier is advanced per-transaction using `GtidPartition` timestamps.
