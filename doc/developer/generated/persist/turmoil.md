---
source: src/persist/src/turmoil.rs
revision: 181b1e7efc
---

# persist::turmoil

Provides `Blob` and `Consensus` implementations for use with the [turmoil](https://github.com/tokio-rs/turmoil) network simulation framework, gated behind the `turmoil` feature flag.
Both implementations forward calls over simulated TCP connections to server-side actors (`serve_blob`, `serve_consensus`) backed by the in-memory `MemBlob` and `MemConsensus` types, allowing turmoil to partition or crash those servers independently.
The `BlobState` and `ConsensusState` shared handles let test code configure and inspect the simulated backends.
