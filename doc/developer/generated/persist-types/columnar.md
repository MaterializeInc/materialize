---
source: src/persist-types/src/columnar.rs
revision: e757b4d11b
---

# persist-types::columnar

Defines the core columnar encoding abstraction: `Schema<T>`, `ColumnEncoder<T>`, `ColumnDecoder<T>`, and `FixedSizeCodec<T>`.
`Schema<T>` maps a `Codec` type to a concrete Arrow column type and provides factory methods for the associated encoder and decoder, amortizing downcasting over all rows in a part rather than once per row.
Also provides `codec_to_schema` and `schema_to_codec` helpers for converting between binary-encoded and structured Arrow representations.
