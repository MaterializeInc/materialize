---
source: src/storage-types/src/errors.rs
revision: af9155582e
---

# storage-types::errors

Defines the error taxonomy for storage dataflows, including `DecodeError` (malformed data), `DecodeErrorKind`, `DataflowError` (wraps decode errors, eval errors, and resource exhaustion), and connection-establishment errors such as `ContextCreationError` and `CsrConnectError`.
All types implement protobuf serialization via `mz_proto::RustType` and can be persisted as part of the error stream alongside source data.
