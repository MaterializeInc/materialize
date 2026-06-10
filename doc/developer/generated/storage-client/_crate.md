---
source: src/storage-client/src/lib.rs
revision: a986ee0c28
---

# storage-client

Provides the public API and shared types for Materialize's storage layer, used by both the storage controller (`mz-storage-controller`) and storage workers (`mz-storage-operators`/`mz-clusterd`).

## Module structure

* `client` — `StorageClient` trait and `StorageCommand`/`StorageResponse` protocol types
* `controller` — `StorageController` trait and collection lifecycle types (`CollectionDescription`, `DataSource`, `ExportState`, `MonotonicAppender`, …)
* `healthcheck` — `RelationDesc` schemas for introspection status/history collections
* `metrics` — Prometheus metrics for the controller and its replica connections
* `sink` — Kafka topic and schema-registry helpers for sink setup
* `statistics` — Per-worker source/sink statistics structs and schemas
* `storage_collections` — `StorageCollections` implementation managing persist since handles and shard finalization
* `util::remap_handle` — `RemapHandle` traits for reclocking

## Key dependencies

`mz-storage-types`, `mz-persist-client`, `mz-persist-types`, `mz-repr`, `mz-service`, `mz-cluster-client`, `mz-txn-wal`, `mz-kafka-util`, `mz-ccsr`.

## Downstream consumers

`mz-storage-controller`, `mz-storage-operators`, `mz-clusterd`, `mz-controller`, `mz-adapter`.
