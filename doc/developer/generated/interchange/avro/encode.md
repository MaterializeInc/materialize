---
source: src/interchange/src/avro/encode.rs
revision: 95ba315ab3
---

# interchange::avro::encode

Provides `AvroEncoder` (implements `Encode`) and `AvroSchemaGenerator` for encoding Materialize `Row`s to Avro bytes, as well as `encode_datums_as_avro`, `encode_debezium_transaction_unchecked`, and `get_debezium_transaction_schema` for producing Debezium-compatible transaction metadata.
Schema generation delegates to `build_row_schema_json` from the `json` module and handles Confluent wire-format framing.
