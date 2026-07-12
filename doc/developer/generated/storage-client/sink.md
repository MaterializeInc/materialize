---
source: src/storage-client/src/sink.rs
revision: 9d96aa256e
---

# storage-client::sink

Provides Kafka-sink utility functions used during sink setup: `ensure_kafka_topic` creates or verifies a Kafka topic with the appropriate partition count and replication factor (including discovery of broker defaults); `publish_kafka_schema` publishes an Avro schema to the Confluent Schema Registry with optional compatibility-level configuration; and `publish_glue_schema` publishes an Avro schema to an AWS Glue Schema Registry, returning the schema-version UUID to frame records with. `publish_glue_schema` reuses an already-registered definition to avoid duplicate versions on restart, creates the schema on first publish with a configurable compatibility policy (defaulting to `BACKWARD`), and polls until Glue's asynchronous compatibility check resolves the version to `Available`.
Also defines the `progress_key` submodule with `ProgressKey`, which encodes a sink's `GlobalId` as a Kafka message key for progress tracking.
