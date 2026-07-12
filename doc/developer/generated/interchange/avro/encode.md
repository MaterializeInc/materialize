---
source: src/interchange/src/avro/encode.rs
revision: 57cbba8867
---

# interchange::avro::encode

Provides `AvroEncoder` (implements `Encode`) and `AvroSchemaGenerator` for encoding Materialize `Row`s to Avro bytes, as well as `encode_datums_as_avro`, `encode_debezium_transaction_unchecked`, and `get_debezium_transaction_schema` for producing Debezium-compatible transaction metadata.
Schema generation delegates to `build_row_schema_json` from the `json` module.
`AvroSchemaId` is a public enum that selects the wire-format framing the encoder writes and strips: `AvroSchemaId::Confluent(i32)` uses the Confluent magic byte plus 4-byte big-endian schema id, and `AvroSchemaId::Glue(Uuid)` uses the 18-byte AWS Glue header (via `crate::glue::write_avro_header`). The `schema_id` field on `AvroEncoder` is fixed at construction and must agree between encoding and hashing, since `hash` strips the same header that `encode_unchecked` writes.
