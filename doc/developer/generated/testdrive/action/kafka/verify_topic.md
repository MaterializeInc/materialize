---
source: src/testdrive/src/action/kafka/verify_topic.rs
revision: f38003ddc8
---

# testdrive::action::kafka::verify_topic

Implements the `kafka-verify-topic` builtin command, which verifies that a Kafka topic's configuration matches expected values.
Can resolve a topic from a Materialize sink name or use a literal topic name; retries until expected key-value configuration entries are present or the timeout expires.
