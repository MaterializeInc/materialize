---
source: src/storage-types/src/sources/kafka.rs
revision: 4267863081
---

# storage-types::sources::kafka

Defines `KafkaSourceConnection` (topic, start offsets, group-ID prefix, metadata columns, refresh interval) and the `KafkaTimestamp` type alias (`Partitioned<RangeBound<i32>, MzOffset>`) that models Kafka's per-partition offset timeline.
Also defines `KafkaSourceExportDetails`, `KafkaMetadataKind`, `RangeBound`, and the `KafkaTopicOptions` type.
Provides `KafkaSourceConnection::fetch_write_frontier` for querying the current high-water mark of all partitions.
