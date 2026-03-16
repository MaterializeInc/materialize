---
source: src/sql/src/kafka_util.rs
revision: 2a6ac3ab4c
---

# mz-sql::kafka_util

Generates `KafkaSourceConfigOptionExtracted` and `KafkaSinkConfigOptionExtracted` via `generate_extracted_config!`, parsing `WITH` option values for Kafka sources and sinks respectively (topic, offsets, compression type, etc.).
Also provides async helpers for fetching Kafka topic watermarks, used during purification of Kafka source `START OFFSET` options.
