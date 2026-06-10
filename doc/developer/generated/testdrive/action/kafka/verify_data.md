---
source: src/testdrive/src/action/kafka/verify_data.rs
revision: f23bdd4c1d
---

# testdrive::action::kafka::verify_data

Implements the `kafka-verify-data` builtin command, which consumes messages from a Kafka topic and compares them against expected rows.
Supports Avro and byte/text message decoding; optionally checks message headers, verifies row ordering, and applies regex substitution to decoded values before comparison.
