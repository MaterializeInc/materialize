---
source: src/interchange/src/avro.rs
revision: 57cbba8867
---

# interchange::avro

Aggregates Avro encoding, decoding, and schema conversion for the interchange layer.
Re-exports `Decoder`, `DiffPair` (decode), `AvroEncoder`, `AvroSchemaGenerator`, `AvroSchemaId`, `encode_datums_as_avro`, `encode_debezium_transaction_unchecked`, `get_debezium_transaction_schema` (encode), and `AvroSchemaResolver`, `WriterSchemaKey`, `WriterSchemaProvider`, `parse_schema`, `schema_to_relationdesc` (schema).
The three child modules (`decode`, `encode`, `schema`) handle their respective concerns independently, sharing the `is_null` helper defined here.
