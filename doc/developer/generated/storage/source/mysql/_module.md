---
source: src/storage/src/source/mysql.rs
revision: e79a6d96d9
---

# mz-storage::source::mysql

Implements `SourceRender` for `MySqlSourceConnection`, composing snapshot, replication, and statistics operators into a complete MySQL CDC ingestion dataflow.
The snapshot operator takes consistent per-worker table snapshots using table locks and GTID-based timestamps; the replication operator reads the binlog from a single worker using GTID-partitioned timestamps; the statistics operator probes the server to track offset progress.
Definite errors (bad column data) flow into the per-export error collection; transient errors (connection failures) trigger a dataflow restart via the health system.
