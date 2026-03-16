---
source: src/persist/src/foundationdb.rs
revision: 4267863081
---

# persist::foundationdb

Implements the `Consensus` trait backed by FoundationDB, gated behind the `foundationdb` feature flag.
State is stored in a FDB directory subspace: a `./keys/<key>` entry tracks existing keys and `./data/<key>/<seqno>` maps sequence numbers to data blobs.
The current sequence number for a key is determined by a reverse range scan rather than a separate head pointer, keeping data and metadata local.
FDB retryable errors are mapped to `Indeterminate` and non-retryable errors to `Determinate`.
