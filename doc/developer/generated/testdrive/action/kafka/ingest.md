---
source: src/testdrive/src/action/kafka/ingest.rs
revision: f23bdd4c1d
---

# testdrive::action::kafka::ingest

Implements the `kafka-ingest` builtin command, which produces messages to a Kafka topic in one of several formats.
Supported key and value formats include bytes, text, Avro (with Confluent framing), and Protobuf (with or without Confluent framing); messages may carry custom headers.
Records are parsed from the command's input block (one JSON object per line for Avro/Protobuf, one raw line for text/bytes) and produced concurrently via rdkafka's `FutureProducer`.
