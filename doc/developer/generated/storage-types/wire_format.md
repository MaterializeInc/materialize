---
source: src/storage-types/src/wire_format.rs
revision: 9c9b2926db
---

# storage-types::wire_format

Wire-format dispatch for Avro encoding/decoding on Kafka topics.

`WireFormat<C>` is an enum with three variants selecting how schema identifiers are framed inside a Kafka record payload:

- `None` — no framing; the single schema carried by the surrounding `AvroEncoding` applies to every record. Source-only; never constructed for a sink.
- `Confluent { registry: Option<C::Csr> }` — Confluent Schema Registry framing (magic byte + i32 schema id). `registry` is `None` for sources that ingest Confluent-framed bytes without an attached CSR connection.
- `Glue { registry: Option<C::GlueSchemaRegistry> }` — AWS Glue Schema Registry framing (magic byte + UUID schema-version id). Not yet constructable from SQL; defined so the in-memory shape is stable while planner support is added.

`AlterCompatible` is implemented to allow altering only within the same variant and only when the registry is compatible; cross-variant changes and changes between `Some` and `None` on either side are rejected with `AlterError`.

`IntoInlineConnection` is implemented for `WireFormat<ReferencedConnection>` to resolve each variant's optional connection reference.
