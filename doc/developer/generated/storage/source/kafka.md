---
source: src/storage/src/source/kafka.rs
revision: 944339446e
---

# mz-storage::source::kafka

Implements `SourceRender` for `KafkaSourceConnection` via `KafkaSourceReader`.
Uses librdkafka's `BaseConsumer` with per-partition queues, distributing partitions across workers, and emits `SourceMessage` records timestamped with `Partitioned<RangeBound<PartitionId>, MzOffset>`.
Handles metadata fetching, offset seeking, SSH tunnel health monitoring, Kafka statistics reporting, and optional metadata extraction (offset, partition, timestamp, headers).
When the `KAFKA_LOW_WATERMARK_CHECK` dyncfg flag is enabled, the reader fetches partition low watermarks at rehydration time and stalls with an error (emitting a `HealthStatusUpdate::stalled` and a definite `SourceError`) if the start offset or resume upper has been compacted away by Kafka; stalling (rather than halting) prevents the healthcheck operator from issuing a restart that would advance the resume upper past the compacted region and produce a zombie source.
The upstream frontier is probed via the `Ticker` and pushed as `Probe` events for reclocking.
