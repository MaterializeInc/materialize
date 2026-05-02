---
source: src/storage/src/server.rs
revision: 44a09cff14
---

# mz-storage::server

Implements `serve`, the crate's public entry point that initialises a timely cluster for the storage layer.
It registers `StorageMetrics`, constructs a `Config` with persist clients, txn-wal context, connection context, and shared RocksDB write-buffer manager, builds the timely cluster via the `ClusterSpec` trait, and returns a factory closure that creates `StorageClient` connections to the running cluster.
`serve` accepts an optional `timely_log_writers` vector (one `TimelyLogWriter` per local worker) that, when non-empty, causes each storage worker to register a timely logger that forwards batched `TimelyEvent`s to the corresponding compute logging dataflow.
Before forwarding, `remap_timely_event_ids` offsets all operator, channel, and address IDs by `STORAGE_ID_OFFSET` (1 << 48) to prevent collisions with compute IDs, and `Park` events are filtered out because they are not meaningful in a multi-runtime context.
`TimelyLogWriter` is a type alias for `Arc<EventLink<mz_repr::Timestamp, Vec<(Duration, TimelyEvent)>>>`.
