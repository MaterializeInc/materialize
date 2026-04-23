---
source: src/storage-client/src/storage_collections.rs
revision: 2bbb5eb8ee
---

# storage-client::storage_collections

Implements `StorageCollections`, the concrete type responsible for managing persist shards for storage collections: holding critical since handles, tracking read capabilities, managing shard finalization for dropped collections, coordinating schema evolution, and exposing collection metadata and frontier information to the controller.
The module also houses the `StorageCollections` trait (the interface) and a background task that performs asynchronous shard finalization.
Its `metrics` submodule tracks finalization counters and shard-set sizes.
