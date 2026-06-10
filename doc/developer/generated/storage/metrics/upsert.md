---
source: src/storage/src/metrics/upsert.rs
revision: 18fb1f796d
---

# mz-storage::metrics::upsert

Defines `UpsertMetricDefs`, `UpsertMetrics`, `UpsertSharedMetrics`, and `UpsertBackpressureMetricDefs` for the upsert operator.
Covers rehydration latency/count, snapshot-merge batch sizes, insert/update/delete counters, multi-get/multi-put latency histograms, and RocksDB-specific variants.
Shared per-source histograms (merge snapshot, multi-get, multi-put latencies) are managed via `Arc<Mutex<BTreeMap<GlobalId, Weak<...>>>>` so that multiple timely workers share the same time-series.
`UpsertBackpressureMetricDefs` tracks backpressure byte flow for upsert dataflows.
