---
source: src/persist-client/src/internal/encoding.rs
revision: 95baa04a85
---

# persist-client::internal::encoding

Implements proto encoding/decoding for all persist internal types: `State`, `StateDiff`, `Trace`, `HollowBatch`, rollups, reader/writer state, and schemas.
Provides `LazyProto` (deferred proto decoding), `LazyPartStats` (deferred stats decoding), and `Schemas` (a pair of key/value schema handles).
`LazyPartStats` stores encoded stats bytes without validating them on ingestion. Its `Debug` impl uses `try_decode()` to surface malformed encodings as a rendered error rather than panicking, since the bytes may arrive straight off blob and pass through `Trace::unflatten`'s validation path before any decode is attempted. Stats from a newer version that use an unknown `oneof` variant decode as an error rather than as a misread value; every read path therefore fails open on them (the filter keeps the part, `stats()` accessors return `None`, and `EXPLAIN FILTER PUSHDOWN` reports the part as selected).
Also owns codec version compatibility checks that prevent reading data written by a newer version of persist.
