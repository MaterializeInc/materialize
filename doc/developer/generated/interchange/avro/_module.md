---
source: src/interchange/src/avro.rs
revision: f23bdd4c1d
---

# interchange::avro

Aggregates Avro encoding, decoding, and schema conversion for the interchange layer.
Re-exports `Decoder`, `DiffPair` (decode), `AvroEncoder`, `AvroSchemaGenerator`, `encode_datums_as_avro`, `encode_debezium_transaction_unchecked`, `get_debezium_transaction_schema` (encode), and `ConfluentAvroResolver`, `parse_schema`, `schema_to_relationdesc` (schema).
The three child modules (`decode`, `encode`, `schema`) handle their respective concerns independently, sharing the `is_null` helper defined here.
