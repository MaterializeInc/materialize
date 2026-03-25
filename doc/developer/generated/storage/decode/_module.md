---
source: src/storage/src/decode.rs
revision: e79a6d96d9
---

# mz-storage::decode

Provides the decoding layer that converts raw byte streams from sources into typed `Row` values.
The module's two public functions, `render_decode_delimited` and `render_decode_cdcv2`, build timely dataflow operators that drive format-specific decoder state machines (`AvroDecoderState`, `CsvDecoderState`, `ProtobufDecoderState`, and simple inline decoders for bytes/text/JSON/regex).
`render_decode_delimited` handles pre-delimited message-per-record sources such as Kafka, while `render_decode_cdcv2` reconstructs a differential collection from CDCv2-encoded Avro streams.
The `YieldingIter` helper prevents long-running decode loops from blocking the timely scheduler.
