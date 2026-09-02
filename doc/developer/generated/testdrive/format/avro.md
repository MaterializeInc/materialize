---
source: src/testdrive/src/format/avro.rs
revision: 92cc9ce805
---

# testdrive::format::avro

Re-exports Avro schema and value types from `mz_avro` and `mz_interchange`, and provides helper functions for testdrive's Avro encoding/decoding needs.
`from_json` converts a `serde_json::Value` to an `mz_avro::types::Value` guided by a schema node, covering all primitive and complex Avro types.
`from_confluent_bytes` decodes an Avro datum from Confluent wire format (magic byte + 4-byte schema ID + datum).
`from_glue_bytes` decodes an Avro datum from AWS Glue wire format, stripping the 18-byte header (1 magic byte + 1 header version + 16-byte schema-version UUID) before decoding the payload with the provided schema. Callers that also need the schema-version UUID should extract it separately via `mz_interchange::glue::extract_avro_header`.
`DebugValue` wraps `Value` to print timestamps and dates in a human-readable form alongside their raw numeric representations.
