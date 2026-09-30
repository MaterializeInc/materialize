---
source: src/storage-types/src/errors.rs
revision: 4c45b862e2
---

# storage-types::errors

Defines the error taxonomy for storage dataflows, including `DecodeError` (malformed data) and `DecodeErrorKind`, envelope errors (`EnvelopeError`, `UpsertError`, `UpsertValueError`, `UpsertNullKeyError`), source-wide durable errors (`SourceError`, `SourceErrorDetails`), and `DataflowError` which unifies all of these alongside `EvalError`.
Also defines connection-establishment errors `ContextCreationError` and `CsrConnectError`, and a `ContextCreationErrorExt` trait for ergonomic error mapping.
All types implement protobuf serialization via `mz_proto::RustType` and can be persisted as part of the error stream alongside source data.
The columnation impl for `EvalError` handles `InvalidRangeError::InvalidRangeData` alongside the other cloneable range error variants.
`SourceError` carries an optional `hint: Option<Box<str>>` field alongside the error message. When serialized to protobuf, a `None` hint leaves no record of the field in the bytes, so adding the field is invisible for errors that do not set it.
