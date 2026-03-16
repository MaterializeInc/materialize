---
source: src/avro/src/writer.rs
revision: 96f48dd2ed
---

Provides the `Writer<W>` type for producing Avro Object Container Files.
On the first `append` call, `Writer` emits the OCF header containing the magic bytes, a JSON-serialised schema, an optional codec name, and a 16-byte random sync marker; subsequent records are buffered and flushed as compressed blocks once the buffer exceeds `SYNC_INTERVAL` bytes.
`Writer::append_to` can re-open an existing file by reading its header and seeking to the end, allowing multi-session writes with a consistent sync marker.
`to_avro_datum` and `write_avro_datum` are standalone helpers that encode a single validated datum without OCF framing, for use in contexts such as Confluent Schema Registry wire format.
`ValidationError` is returned when a `Value` fails `Value::validate` before encoding.
