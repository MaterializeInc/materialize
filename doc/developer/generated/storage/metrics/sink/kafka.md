---
source: src/storage/src/metrics/sink/kafka.rs
revision: cc81d0177d
---

# mz-storage::metrics::sink::kafka

Defines `KafkaSinkMetricDefs` and `KafkaSinkMetrics`, exposing librdkafka producer statistics (message and byte counts, in-flight requests, error/retry/timeout counts, connection events) plus Materialize-specific counters for progress records and partition count, all labeled by `sink_id`.
