---
source: src/storage-types/src/errors.rs
revision: f498b6e141
---

# storage-types::errors

Defines the error taxonomy for storage dataflows, including `DecodeError` (malformed data) and `DecodeErrorKind`, envelope errors (`EnvelopeError`, `UpsertError`, `UpsertValueError`, `UpsertNullKeyError`), source-wide durable errors (`SourceError`, `SourceErrorDetails`), and `DataflowError` which unifies all of these alongside `EvalError`.
Also defines connection-establishment errors `ContextCreationError` and `CsrConnectError`, and a `ContextCreationErrorExt` trait for ergonomic error mapping.
All types implement protobuf serialization via `mz_proto::RustType` and can be persisted as part of the error stream alongside source data.
