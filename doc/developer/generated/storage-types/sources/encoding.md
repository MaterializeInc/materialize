---
source: src/storage-types/src/sources/encoding.rs
revision: 243171af3f
---

# storage-types::sources::encoding

Defines `SourceDataEncoding` (wrapping optional key and mandatory value `DataEncoding`) and `DataEncoding`, an enum of supported source decoding formats (Avro, Protobuf, CSV, Regex, Bytes, Text, JSON).
Provides `desc()` to derive the `RelationDesc` for each format and implements `IntoInlineConnection` and `AlterCompatible`.
For the `Regex` encoding, `desc()` calls `non_nullable_capture_groups()` on the compiled regex to identify capture groups that unconditionally participate in every match, and marks those columns as non-nullable in the resulting `RelationDesc`.
