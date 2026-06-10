---
source: src/avro/src/decode.rs
revision: 4267863081
---

Implements schema-driven binary decoding via a visitor-style trait hierarchy.
`AvroDecode` is the central trait; callers implement it to receive decoded scalar values, or delegate sub-structures through `AvroRecordAccess`, `AvroArrayAccess`, `AvroMapAccess`, and `AvroFieldAccess`, enabling zero-copy decoding directly into Rust types without materialising an intermediate `Value`.
`GeneralDeserializer` drives the decode loop by matching the resolved `SchemaNode` to the appropriate decoding path; `give_value` feeds a pre-built `Value` back through an `AvroDecode` implementation, used when supplying default field values.
`TrivialDecoder` and `ValueDecoder` (re-exported from `public_decoders`) provide ready-made implementations: the former discards data, the latter produces `Value`.
The `Skip` trait extends `Read` with an efficient seek-forward operation, and `AvroRead` is a blanket alias combining `Read + Skip`.
