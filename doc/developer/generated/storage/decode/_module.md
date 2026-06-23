---
source: src/storage/src/decode.rs
revision: ae8f529217
---

# mz-storage::decode

Provides the decoding layer that converts raw byte streams from sources into typed `Row` values.
The module's two public functions, `render_decode_delimited` and `render_decode_cdcv2`, build timely dataflow operators that drive format-specific decoder state machines (`AvroDecoderState`, `ProtobufDecoderState`, and simple inline decoders for bytes/text/JSON/regex). The `CsvDecoderState` is imported from `mz_storage_types::sources::encoding` rather than defined locally.
`render_decode_delimited` handles pre-delimited message-per-record sources such as Kafka, while `render_decode_cdcv2` reconstructs a differential collection from CDCv2-encoded Avro streams.
The `YieldingIter` helper prevents long-running decode loops from blocking the timely scheduler.
The `decode` submodule contains only `avro` and `protobuf` decoders; there is no `csv` submodule.
`WireFormat` variants are mapped to a `WriterSchemaProvider` before the decoder is constructed: `WireFormat::None` produces `WriterSchemaProvider::None`, `WireFormat::Confluent` resolves the optional CSR connection and calls `WriterSchemaProvider::confluent`, and `WireFormat::Glue` loads an AWS SDK config, constructs a Glue client, and calls `WriterSchemaProvider::glue`.
