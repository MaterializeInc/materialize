---
source: src/storage-types/src/sources/encoding.rs
revision: f23bdd4c1d
---

# storage-types::sources::encoding

Defines `SourceDataEncoding` (wrapping optional key and mandatory value `DataEncoding`) and `DataEncoding`, an enum of supported source decoding formats (Avro, Protobuf, CSV, Regex, Bytes, Text, JSON).
Provides `desc()` to derive the `RelationDesc` for each format and implements `IntoInlineConnection` and `AlterCompatible`.
