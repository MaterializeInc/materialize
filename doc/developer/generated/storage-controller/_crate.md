---
source: src/storage-controller/src/lib.rs
revision: a60edac7f1
---

# storage-controller

Implements `Controller`, the concrete storage controller that satisfies the `StorageController` trait defined in `mz-storage-client`.
The controller manages the lifecycle of all sources, sinks, and tables: it maintains per-collection metadata, coordinates writes to table shards via `PersistTableWriteWorker` and the txn-wal system, drives introspection collection updates through `CollectionManager`, and dispatches ingestion and sink commands to storage instances (clusters).

## Module structure

* `lib.rs` — `Controller` struct and the full `StorageController` trait implementation; crate entry point.
* `instance` — `Instance` and `Replica`: manages one storage cluster and its replica connections, including scheduling and command replay.
* `history` — `CommandHistory`: reducible command log used to rehydrate replicas.
* `collection_mgmt` — `CollectionManager`: background tasks that maintain append-only and differential introspection collections.
* `persist_handles` — `PersistTableWriteWorker`: serializes table writes through txn-wal; includes read-only mode fallback.
* `statistics` — Background scrapers that flush source and sink statistics into managed collections. When a replica is dropped, the controller writes a `Status::Paused` update for that replica. `mz_source_statuses` and `mz_sink_statuses` rely on this convention: a per-replica `paused` event means the replica was dropped, and the views retain such events as terminal drop markers.
* `rtr` — Real-time recency timestamp resolution for external source connections.

## Key dependencies

`mz-storage-client` (traits and client types), `mz-storage-types` (collection descriptors, error types), `mz-persist-client` (shard I/O), `mz-txn-wal` (transactional table writes), `mz-service` (gRPC transport), `mz-cluster-client` (replica location).

## Downstream consumers

`mz-controller` (the combined compute+storage controller exposed to the adapter), `mz-environmentd`.
