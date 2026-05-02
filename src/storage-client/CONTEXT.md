# mz-storage-client

Public API and shared types for Materialize's storage layer. Consumed by the
storage controller, storage workers, the combined controller, and the adapter.

## Subtree (≈ 7,176 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/storage_collections.rs` | 3,342 | `StorageCollections` trait + `StorageCollectionsImpl` |
| `src/statistics.rs` | 1,185 | Per-worker source/sink statistics structs and schemas |
| `src/controller.rs` | 959 | `StorageController` trait, `CollectionDescription`, `DataSource`, `IntrospectionType` |
| `src/client.rs` | 641 | `StorageClient` trait, `StorageCommand`/`StorageResponse` protocol |
| `src/metrics.rs` | 377 | Prometheus metrics for controller and replica connections |
| `src/sink.rs` | 280 | Kafka topic and schema-registry helpers for sink setup |
| `src/healthcheck.rs` | 206 | `RelationDesc` schemas for introspection status/history collections |
| `src/util/remap_handle.rs` | 54 | `RemapHandle` trait for reclocking |

## Package identity

Crate name: `mz-storage-client`. Key deps: `mz-storage-types`, `mz-persist-client`,
`mz-persist-types`, `mz-repr`, `mz-service`, `mz-cluster-client`, `mz-txn-wal`,
`mz-kafka-util`, `mz-ccsr`.

## Purpose

Acts as the contract crate between the storage controller and storage workers.
It defines both the **wire protocol** (`StorageCommand`/`StorageResponse`) and
the **controller-side trait** (`StorageController`), plus the persist-side
collection management abstraction (`StorageCollections`).

## Key interfaces (exported)

- `StorageController` trait — lifecycle management: `create_collections`,
  `create_instance`, `connect_replica`, `drop_sources`, `drop_sinks`,
  `append_table`, `update_read_capabilities`, `ready`, `process`.
- `StorageCollections` trait — since/upper frontier tracking, `ReadHold`
  management, `snapshot_stats`, `prepare_state`.
- `StorageCollectionsImpl` — concrete implementation (lives here, not in
  `mz-storage-controller`; instantiated by `mz-controller`).
- `StorageClient` trait — thin `GenericClient<StorageCommand, StorageResponse>`.
- `StorageCommand` / `StorageResponse` — gRPC wire types for controller↔worker.
- `DataSource` / `CollectionDescription` / `IntrospectionType` — collection
  lifecycle types.
- `MonotonicAppender` — channel for adapter-side append-only writes.

## Architecture notes

- **Impl-in-client seam**: `StorageCollectionsImpl` is a concrete implementation
  inside the client crate (3,342 LOC), not in `mz-storage-controller`. It is
  instantiated by `mz-controller` and injected into `mz-storage-controller` as
  `Arc<dyn StorageCollections>`. This enables `mz-controller` to share the
  since-handle logic between storage and compute layers, but means the client
  crate carries significant stateful implementation weight.
- **Dual collection stores**: `mz-storage-controller` maintains its own
  `BTreeMap<GlobalId, CollectionState>` (ingestion/export/wallclock-lag state)
  alongside `StorageCollectionsImpl`'s `BTreeMap<GlobalId, CollectionState>`
  (persist since/upper/ReadHold state). These are kept in sync via explicit
  notifications — a source of coupling.
- **`IntrospectionType`** enum (24 variants) lives in `controller.rs` and spans
  storage, compute, and statement-logging concerns. It is a mild layer
  overspill but is kept here for `StorageController` trait completeness.

## Downstream consumers

`mz-storage-controller`, `mz-storage-operators`, `mz-clusterd`, `mz-controller`,
`mz-adapter`.

## Bubbled findings for src/CONTEXT.md

- `mz-storage-client` is the central contract crate for the storage layer:
  both the wire protocol and the controller trait live here.
- `StorageCollectionsImpl` being inside the client crate (not the controller)
  is a deliberate inversion — it allows `mz-controller` to own the object and
  share it across compute and storage paths.
- Dual `CollectionState` maps (one in client, one in controller) require explicit
  synchronization; any future refactor should evaluate merging them.
