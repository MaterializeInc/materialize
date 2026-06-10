---
source: src/storage/src/decode/protobuf.rs
revision: ce1c5ae5e3
---

# mz-storage::decode::protobuf

Wraps `mz_interchange::protobuf::Decoder` in a `ProtobufDecoderState` struct that decodes byte slices into `Row` values using pre-validated descriptor bytes and message names.
It is used as a `PreDelimitedFormat::Protobuf` variant in the decode pipeline.
