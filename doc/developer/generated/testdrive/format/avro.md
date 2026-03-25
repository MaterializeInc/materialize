---
source: src/testdrive/src/format/avro.rs
revision: f38003ddc8
---

# testdrive::format::avro

Re-exports Avro schema and value types from `mz_avro` and `mz_interchange`, and provides helper functions for testdrive's Avro encoding/decoding needs.
`from_json` converts a `serde_json::Value` to an `mz_avro::types::Value` guided by a schema node, covering all primitive and complex Avro types.
`from_confluent_bytes` decodes an Avro datum from Confluent wire format (magic byte + 4-byte schema ID + datum).
`DebugValue` wraps `Value` to print timestamps and dates in a human-readable form alongside their raw numeric representations.
