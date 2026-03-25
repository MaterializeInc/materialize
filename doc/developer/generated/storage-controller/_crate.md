---
source: src/storage-controller/src/lib.rs
revision: 82d92a7fad
---

# storage-controller

Implements `Controller<T>`, the concrete storage controller that satisfies the `StorageController` trait defined in `mz-storage-client`.
The controller manages the lifecycle of all sources, sinks, and tables: it maintains per-collection metadata, coordinates writes to table shards via `PersistTableWriteWorker` and the txn-wal system, drives introspection collection updates through `CollectionManager`, and dispatches ingestion and sink commands to storage instances (clusters).

## Module structure

* `lib.rs` — `Controller<T>` struct and the full `StorageController` trait implementation; crate entry point.
* `instance` — `Instance<T>` and `Replica<T>`: manages one storage cluster and its replica connections, including scheduling and command replay.
* `history` — `CommandHistory<T>`: reducible command log used to rehydrate replicas.
* `collection_mgmt` — `CollectionManager<T>`: background tasks that maintain append-only and differential introspection collections.
* `persist_handles` — `PersistTableWriteWorker<T>`: serializes table writes through txn-wal; includes read-only mode fallback.
* `statistics` — Background scrapers that flush source and sink statistics into managed collections.
* `rtr` — Real-time recency timestamp resolution for external source connections.

## Key dependencies

`mz-storage-client` (traits and client types), `mz-storage-types` (collection descriptors, error types), `mz-persist-client` (shard I/O), `mz-txn-wal` (transactional table writes), `mz-service` (gRPC transport), `mz-cluster-client` (replica location).

## Downstream consumers

`mz-controller` (the combined compute+storage controller exposed to the adapter), `mz-environmentd`.
