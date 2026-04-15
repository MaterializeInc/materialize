---
source: src/storage/src/source/kafka.rs
revision: b0fa98e931
---

# mz-storage::source::kafka

Implements `SourceRender` for `KafkaSourceConnection` via `KafkaSourceReader`.
Uses librdkafka's `BaseConsumer` with per-partition queues, distributing partitions across workers, and emits `SourceMessage` records timestamped with `Partitioned<RangeBound<PartitionId>, MzOffset>`.
Handles metadata fetching, offset seeking, SSH tunnel health monitoring, Kafka statistics reporting, and optional metadata extraction (offset, partition, timestamp, headers).
The upstream frontier is probed via the `Ticker` and pushed as `Probe` events for reclocking.
