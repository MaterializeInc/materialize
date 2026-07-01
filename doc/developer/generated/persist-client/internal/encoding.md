---
source: src/persist-client/src/internal/encoding.rs
revision: 25802a51f9
---

# persist-client::internal::encoding

Implements proto encoding/decoding for all persist internal types: `State`, `StateDiff`, `Trace`, `HollowBatch`, rollups, reader/writer state, and schemas.
Provides `LazyProto` (deferred proto decoding), `LazyPartStats` (deferred stats decoding), and `Schemas` (a pair of key/value schema handles).
Also owns codec version compatibility checks that prevent reading data written by a newer version of persist.
