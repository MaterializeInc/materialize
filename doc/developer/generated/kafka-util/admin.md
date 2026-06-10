---
source: src/kafka-util/src/admin.rs
revision: d2a65dde8a
---

# mz-kafka-util::admin

Provides higher-level wrappers around `rdkafka`'s admin API for topic management.
`ensure_topic` creates a Kafka topic if absent and waits for broker metadata to reflect the correct partition count; `delete_topic` / `delete_existing_topic` delete a topic and wait for it to disappear from metadata.
`ensure_topic_config` / `get_topic_config` / `alter_topic_config` inspect and optionally reconcile topic configuration with desired values, governed by the `EnsureTopicConfig` enum (`Skip`, `Check`, `Alter`).
`DeleteTopicError` enumerates failure modes including Kafka errors, wrong result counts, and topic resurrection after deletion.
