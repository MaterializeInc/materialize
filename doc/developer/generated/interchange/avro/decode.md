---
source: src/interchange/src/avro/decode.rs
revision: 9c9b2926db
---

# interchange::avro::decode

Provides `Decoder`, which decodes Avro-encoded bytes into Materialize `Row`s using the `mz-avro` deserialization framework, with optional Confluent wire-format header stripping via `AvroSchemaResolver`.
Also defines `DiffPair<V>`, a before/after pair used by the Debezium envelope.
