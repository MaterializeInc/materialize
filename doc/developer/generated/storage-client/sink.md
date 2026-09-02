---
source: src/storage-client/src/sink.rs
revision: ba3c29ef7b
---

# storage-client::sink

Provides Kafka-sink utility functions used during sink setup: `ensure_kafka_topic` creates or verifies a Kafka topic with the appropriate partition count and replication factor (including discovery of broker defaults); `publish_kafka_schema` publishes an Avro schema to the Confluent Schema Registry with optional compatibility-level configuration; `publish_glue_schema` publishes an Avro schema to an AWS Glue Schema Registry, returning the schema-version UUID to frame records with; and `glue_compatibility_from_csr` maps a `mz_ccsr::CompatibilityLevel` to its AWS Glue `Compatibility` equivalent (every Confluent level has a Glue analogue, with transitive levels mapping to Glue's `*All` modes). `publish_glue_schema` reuses an already-registered definition to avoid duplicate versions on restart, creates the schema on first publish with a configurable compatibility policy (defaulting to `BACKWARD`), and polls until Glue's asynchronous compatibility check resolves the version to `Available`.
Also defines the `progress_key` submodule with `ProgressKey`, which encodes a sink's `GlobalId` as a Kafka message key for progress tracking.
