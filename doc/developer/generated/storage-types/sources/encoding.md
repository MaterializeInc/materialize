---
source: src/storage-types/src/sources/encoding.rs
revision: ea72b29d42
---

# storage-types::sources::encoding

Defines `SourceDataEncoding` (wrapping optional key and mandatory value `DataEncoding`) and `DataEncoding`, an enum of supported source decoding formats (Avro, Protobuf, CSV, Regex, Bytes, Text, JSON).
Provides `desc()` to derive the `RelationDesc` for each format and implements `IntoInlineConnection` and `AlterCompatible`.
For the `Regex` encoding, `desc()` iterates over the capture group names of the compiled regex (skipping the implicit full-match group at index 0) and produces one nullable `String` column per capture group, using the capture name if present or `columnN` otherwise. All regex capture group columns are marked nullable.

Also defines `CsvDecoderState`, the pure incremental CSV decoder. `CsvDecoderState` lives in this crate (rather than `mz-storage`) so it can be exercised and fuzzed without `mz-storage`'s heavy dependency tree; the `mz-storage` decode operator imports it from here. Each field's bytes are validated as UTF-8 independently: a field whose bytes are not valid UTF-8 on their own is a decode error. The output and ends buffers are cleared after every completed record (success or error) so that a malformed record cannot corrupt subsequent records via stale buffer state.
