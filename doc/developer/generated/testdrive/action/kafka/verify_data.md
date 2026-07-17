---
source: src/testdrive/src/action/kafka/verify_data.rs
revision: 92cc9ce805
---

# testdrive::action::kafka::verify_data

Implements the `kafka-verify-data` builtin command, which consumes messages from a Kafka topic and compares them against expected rows.
Supports Avro and byte/text message decoding; optionally checks message headers, verifies row ordering, and applies regex substitution to decoded values before comparison.

When the `glue` flag is set, Avro records are decoded using AWS Glue framing (an 18-byte header carrying a schema-version UUID) rather than Confluent wire format. Schema resolution in this mode is handled by `resolve_glue_schemas`, which reads the schema-version UUID from the first record on each side and fetches that version's definition from Glue. The `decode_avro` helper centralises the framing choice, delegating to `avro::from_glue_bytes` or `avro::from_confluent_bytes` based on the flag.
