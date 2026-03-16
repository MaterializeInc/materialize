---
source: src/environmentd/src/http/prometheus.rs
revision: 382362f305
---

# environmentd::http::prometheus

Defines `PrometheusSqlQuery` and four static slices of pre-written SQL queries that are executed periodically to expose Materialize state as Prometheus metrics: frontier metrics (`FRONTIER_METRIC_QUERIES`), usage/inventory metrics (`USAGE_METRIC_QUERIES`), per-replica compute arrangement metrics (`COMPUTE_METRIC_QUERIES`), and storage object metrics (`STORAGE_METRIC_QUERIES`).
Each query result row maps to one or more Prometheus time series via column labels.
