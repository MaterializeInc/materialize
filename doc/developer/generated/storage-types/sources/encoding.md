---
source: src/storage-types/src/sources/encoding.rs
revision: 9c9b2926db
---

# storage-types::sources::encoding

Defines `SourceDataEncoding` (wrapping optional key and mandatory value `DataEncoding`) and `DataEncoding`, an enum of supported source decoding formats (Avro, Protobuf, CSV, Regex, Bytes, Text, JSON).
Provides `desc()` to derive the `RelationDesc` for each format and implements `IntoInlineConnection` and `AlterCompatible`.
For the `Regex` encoding, `desc()` iterates over the capture group names of the compiled regex (skipping the implicit full-match group at index 0) and produces one nullable `String` column per capture group, using the capture name if present or `columnN` otherwise. All regex capture group columns are marked nullable.
