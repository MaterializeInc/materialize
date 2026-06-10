---
source: src/storage/src/metrics/source/mysql.rs
revision: cc81d0177d
---

# mz-storage::metrics::source::mysql

Defines `MySqlSourceMetricDefs` and `MySqlSourceMetrics` for replication event counts (inserts, updates, deletes, ignored, total, GTID transaction IDs, table count) plus `MySqlSnapshotMetrics` that records per-table count latency gauges shared between tokio tasks and the replication operator via `Arc<Mutex<>>`.
