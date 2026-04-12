---
source: src/storage/src/server.rs
revision: 3ac449b444
---

# mz-storage::server

Implements `serve`, the crate's public entry point that initialises a timely cluster for the storage layer.
It registers `StorageMetrics`, constructs a `Config` with persist clients, txn-wal context, connection context, and shared RocksDB write-buffer manager, builds the timely cluster via the `ClusterSpec` trait, and returns a factory closure that creates `StorageClient` connections to the running cluster.
