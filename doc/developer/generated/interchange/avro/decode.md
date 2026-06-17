---
source: src/interchange/src/avro/decode.rs
revision: 6e5d1dd316
---

# interchange::avro::decode

Provides `Decoder`, which decodes Avro-encoded bytes into Materialize `Row`s using the `mz-avro` deserialization framework, with optional Confluent wire-format header stripping via `AvroSchemaResolver`.
Also defines `DiffPair<V>`, a before/after pair used by the Debezium envelope.
`Decoder::new` maps its `(ccsr_client, confluent_wire_format)` parameters to a `WriterSchemaProvider` variant before constructing the `AvroSchemaResolver`; `(None, false)` produces `WriterSchemaProvider::None`, and `(_, true)` produces `WriterSchemaProvider::confluent(ccsr_client)`.
