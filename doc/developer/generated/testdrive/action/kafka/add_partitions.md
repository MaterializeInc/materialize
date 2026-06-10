---
source: src/testdrive/src/action/kafka/add_partitions.rs
revision: e757b4d11b
---

# testdrive::action::kafka::add_partitions

Implements the `kafka-add-partitions` builtin command, which increases the number of partitions on an existing Kafka topic.
Retries until the observed partition count reaches or exceeds the requested total, handling the case where the broker-side change propagates asynchronously.
