---
source: src/durable-cache/src/lib.rs
revision: 901d0526a1
---

# mz-durable-cache

Provides a durable key-value cache abstraction built on top of a persist shard.
`DurableCache` maintains a local in-memory copy that is kept in sync with persist via a `Subscribe`, and writes use compare-and-append to detect conflicts and retry.
The `DurableCacheCodec` trait decouples the user-facing key/value types from their persist-encoded forms.
The critical since handle is downgraded on every successful write to keep the shard's compaction frontier advancing.
