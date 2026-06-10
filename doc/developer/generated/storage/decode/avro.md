---
source: src/storage/src/decode/avro.rs
revision: f23bdd4c1d
---

# mz-storage::decode::avro

Wraps the `mz_interchange::avro::Decoder` in an `AvroDecoderState` struct that drives async Avro deserialization and converts results into `Row` values or `DecodeErrorKind` errors.
It is consumed by `decode.rs` as one variant of `DataDecoderInner`.
