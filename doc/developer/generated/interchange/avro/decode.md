---
source: src/interchange/src/avro/decode.rs
revision: f23bdd4c1d
---

# interchange::avro::decode

Provides `Decoder`, which decodes Avro-encoded bytes into Materialize `Row`s using the `mz-avro` deserialization framework, with optional Confluent wire-format header stripping via `ConfluentAvroResolver`.
Also defines `DiffPair<V>`, a before/after pair used by the Debezium envelope.
