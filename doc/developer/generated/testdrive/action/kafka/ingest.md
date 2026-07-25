---
source: src/testdrive/src/action/kafka/ingest.rs
revision: 5b2cefc829
---

# testdrive::action::kafka::ingest

Implements the `kafka-ingest` builtin command, which produces messages to a Kafka topic in one of several formats.
Supported key and value formats include bytes, Avro (with Confluent framing, with AWS Glue Schema Registry framing, or plain), and Protobuf (with or without Confluent framing); messages may carry custom headers.
The AWS Glue framing prepends a 3-byte header (magic byte `0x03`, compression byte `0x00`) followed by the 16-byte schema-version UUID before the Avro payload.
For Confluent Avro, schema references are resolved transitively: direct references and their transitive dependencies are fetched from the registry and parsed in dependency order before the primary schema.
Records are parsed from the command's input block (one JSON object per line for Avro/Protobuf, one raw line for bytes) and produced concurrently via rdkafka's `FutureProducer`.
