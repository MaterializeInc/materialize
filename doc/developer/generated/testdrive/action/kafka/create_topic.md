---
source: src/testdrive/src/action/kafka/create_topic.rs
revision: 24b032ff3d
---

# testdrive::action::kafka::create_topic

Implements the `kafka-create-topic` builtin command.
Creates a Kafka topic with a `testdrive-<name>-<seed>` name, configuring partition count, replication factor, and optional topic-level configuration keys; registers the topic in state so it can be cleaned up after the test.
