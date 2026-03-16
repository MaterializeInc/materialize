---
source: src/storage/src/metrics.rs
revision: 1db0a3421e
---

# mz-storage::metrics

Defines `StorageMetrics`, the top-level metrics container for the `mz-storage` crate.
It bundles definition structs for all metric categories (source, decode, upsert, upsert backpressure, sink, source statistics, sink statistics) and exposes factory methods (`get_source_metrics`, `get_kafka_source_metrics`, `get_upsert_metrics`, `get_kafka_sink_metrics`, `get_iceberg_sink_metrics`, etc.) that instantiate per-id/worker delete-on-drop handles.
The struct can be cloned; all inner definition arcs are shared.
Child submodules cover decode, sink (kafka, iceberg), and source (kafka, mysql, postgres, sql_server) metric definitions.
