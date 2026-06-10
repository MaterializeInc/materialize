---
source: src/storage/src/metrics/source/sql_server.rs
revision: 802229d6eb
---

# mz-storage::metrics::source::sql_server

Defines `SqlServerSourceMetricDefs` and `SqlServerSourceMetrics` for SQL Server CDC event counts (inserts, updates, deletes, ignored) and snapshot-phase metrics (table count, per-table size latency, table lock counts), labeled by `source_id` and `worker_id`.
Per-table snapshot gauges are stored in `Rc<Mutex<BTreeMap>>` and created lazily on first reference.
