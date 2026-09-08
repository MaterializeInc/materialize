---
source: src/avro/src/decode.rs
revision: 18d73eb54d
---

Implements schema-driven binary decoding via a visitor-style trait hierarchy.
`AvroDecode` is the central trait; callers implement it to receive decoded scalar values, or delegate sub-structures through `AvroRecordAccess`, `AvroArrayAccess`, `AvroMapAccess`, and `AvroFieldAccess`, enabling zero-copy decoding directly into Rust types without materialising an intermediate `Value`.
`GeneralDeserializer` drives the decode loop by matching the resolved `SchemaNode` to the appropriate decoding path; `give_value` feeds a pre-built `Value` back through an `AvroDecode` implementation, used when supplying default field values.
`TrivialDecoder` and `ValueDecoder` (re-exported from `public_decoders`) provide ready-made implementations: the former discards data, the latter produces `Value`.
The `Skip` trait extends `Read` with an efficient seek-forward operation, and `AvroRead` is a blanket alias combining `Read + Skip`.
`bound_block_object_count` validates the object count declared in an OCF block header against the block's decompressed payload: when the schema has a proven positive wire-byte floor (`min_encoded_len`), the count times that floor must not exceed the payload length; when the schema is zero-width (such as `null` or a record of only `null` fields), the count weighted by `min_value_nodes` must not exceed `MAX_VALUE_NODES`. This check is a standalone guard on the block header and is not charged against the per-datum `DECODE_NODES` budget.
