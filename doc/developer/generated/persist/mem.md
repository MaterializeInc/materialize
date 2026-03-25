---
source: src/persist/src/mem.rs
revision: 4a1aeff959
---

# persist::mem

Provides in-memory implementations of both `Blob` (`MemBlob`) and `Consensus` (`MemConsensus`) for use in tests and benchmarks.
`MemBlob` wraps a `BTreeMap` behind a `tokio::sync::Mutex` and supports optional tombstone-delete semantics.
`MemConsensus` stores a `Vec<VersionedData>` per key and enforces the same invariants (sequence-number monotonicity, `[0, i64::MAX]` range) as production backends.
`MemMultiRegistry` (test-only) allows multiple `MemBlob` handles to share the same underlying state, simulating concurrent access.
