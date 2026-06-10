---
source: src/storage-client/src/sink.rs
revision: e757b4d11b
---

# storage-client::sink

Provides Kafka-sink utility functions used during sink setup: `ensure_kafka_topic` creates or verifies a Kafka topic with the appropriate partition count and replication factor (including discovery of broker defaults), and `publish_kafka_schema` publishes an Avro schema to the Confluent Schema Registry with optional compatibility-level configuration.
Also defines the `progress_key` submodule with `ProgressKey`, which encodes a sink's `GlobalId` as a Kafka message key for progress tracking.
