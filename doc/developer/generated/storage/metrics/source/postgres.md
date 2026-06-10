---
source: src/storage/src/metrics/source/postgres.rs
revision: 7af9892533
---

# mz-storage::metrics::source::postgres

Defines `PgSourceMetricDefs` and `PgSourceMetrics` for Postgres replication event counts (inserts, updates, deletes, ignored, total, transactions, table count, WAL LSN) plus `PgSnapshotMetrics` for per-table snapshot count latency gauges, shared across tasks via `Arc<Mutex<>>`.
