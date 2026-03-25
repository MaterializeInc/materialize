---
source: src/avro/src/encode.rs
revision: 4db966dfbb
---

Provides low-level, schema-guided encoding of `Value` trees into the Avro binary format.
The public entry points are `encode` (writes into a caller-supplied `Vec<u8>`), `encode_ref` (takes a `SchemaNode` rather than a `&Schema`), and `encode_to_vec` (returns a freshly allocated buffer).
Integers are zigzag-encoded via helpers from `util`; variable-length types (bytes, strings, arrays, maps) are prefixed with their encoded length; union variants are preceded by their index.
No schema validation is performed; callers are expected to have validated the value before encoding.
