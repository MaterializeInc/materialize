---
source: src/testdrive/src/format.rs
revision: 0eb828f5c1
---

# testdrive::format

Thin module that groups the two encoding helpers used when constructing Kafka message payloads.
`avro` handles Avro schema resolution and Confluent-format encoding/decoding; `bytes` handles hex-escaped byte string literals.
