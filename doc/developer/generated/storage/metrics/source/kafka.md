---
source: src/storage/src/metrics/source/kafka.rs
revision: cc81d0177d
---

# mz-storage::metrics::source::kafka

Defines `KafkaSourceMetricDefs` and `KafkaSourceMetrics`, which track per-partition high-watermark offsets (`mz_kafka_partition_offset_max`) for Kafka sources.
`KafkaSourceMetrics::set_offset_max` dynamically creates delete-on-drop gauges for new partitions as they are observed.
