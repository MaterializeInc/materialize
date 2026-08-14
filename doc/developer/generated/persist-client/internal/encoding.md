---
source: src/persist-client/src/internal/encoding.rs
revision: c450b59e6d
---

# persist-client::internal::encoding

Implements proto encoding/decoding for all persist internal types: `State`, `StateDiff`, `Trace`, `HollowBatch`, rollups, reader/writer state, and schemas.
Provides `LazyProto` (deferred proto decoding), `LazyPartStats` (deferred stats decoding), and `Schemas` (a pair of key/value schema handles).
`LazyPartStats` stores encoded stats bytes without validating them on ingestion. Its `Debug` impl uses `try_decode()` to surface malformed encodings as a rendered error rather than panicking, since the bytes may arrive straight off blob and pass through `Trace::unflatten`'s validation path before any decode is attempted.
Also owns codec version compatibility checks that prevent reading data written by a newer version of persist.
