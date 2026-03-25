---
crate: mz_avro
sources:
  - src/avro/src/codec.rs
  - src/avro/src/decode.rs
  - src/avro/src/encode.rs
  - src/avro/src/error.rs
  - src/avro/src/lib.rs
  - src/avro/src/reader.rs
  - src/avro/src/schema.rs
  - src/avro/src/types.rs
  - src/avro/src/util.rs
  - src/avro/src/writer.rs
---

`mz_avro` is Materialize's fork of the `avro-rs` crate, providing full read/write support for the Apache Avro binary format with several Materialize-specific extensions.

The crate is organised around three layers.
The bottom layer (`util`, `error`) supplies zigzag integer codecs, an allocation-cap guard, and a unified error type hierarchy.
The middle layer (`types`, `schema`) defines the `Value` enum (the in-memory Avro datum representation), the `SchemaPiece`/`Schema` arena, and the `resolve_schemas` function that builds a writer-to-reader resolution schema used to translate data across schema evolution.
The top layer (`encode`, `decode`, `codec`, `reader`, `writer`) implements the full Avro Object Container File (OCF) format: `Writer` produces framed, optionally compressed files; `Reader` parses and iterates them; `encode`/`decode` provide the low-level datum serialisation used internally.

A distinctive feature is the `AvroDecode` visitor trait in `decode.rs`, which allows callers to bypass the intermediate `Value` representation and decode binary data directly into Rust types, avoiding heap allocation on the hot path.
Schema resolution is baked into decoding: `SchemaPiece` carries `Resolve*` variants that instruct the decoder how to widen, reorder, or substitute values when writer and reader schemas differ.
Materialize-specific additions include `Value::Json` (for Debezium embedded JSON), `Value::Uuid`, and the full suite of logical-type variants (`Date`, `TimestampMilli`, `TimestampMicro`, `Decimal`).
