---
source: src/storage/src/source/kafka.rs
revision: 8f3bf9dd62
---

# mz-storage::source::kafka

Implements `SourceRender` for `KafkaSourceConnection` via `KafkaSourceReader`.
Uses librdkafka's `BaseConsumer` with per-partition queues, distributing partitions across workers, and emits `SourceMessage` records timestamped with `Partitioned<RangeBound<PartitionId>, MzOffset>`.
Handles metadata fetching, offset seeking, SSH tunnel health monitoring, Kafka statistics reporting, and optional metadata extraction (offset, partition, timestamp, headers).
When the `KAFKA_LOW_WATERMARK_CHECK` dyncfg flag is enabled, the reader fetches partition low watermarks at rehydration time. If the fetch itself fails (e.g. a transient connection error), the reader logs a warning, emits `HealthStatusUpdate::stalled` for all outputs, and proceeds with an empty watermark map rather than failing fatally. If the fetch succeeds but reveals that the start offset or resume upper has been compacted away by Kafka, the reader stalls with a definite `SourceError`; stalling (rather than halting) prevents the healthcheck operator from issuing a restart that would advance the resume upper past the compacted region and produce a zombie source.
The upstream frontier is probed via the `Ticker` and pushed as `Probe` events for reclocking.
