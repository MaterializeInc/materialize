# mz-storage-controller

Concrete implementation of `StorageController` (trait defined in
`mz-storage-client`). Coordinates the lifecycle of all sources, sinks, and
tables; owns storage cluster management and persist I/O orchestration.

## Subtree (≈ 8,960 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/lib.rs` | 4,070 | `Controller` struct + full `StorageController` impl |
| `src/collection_mgmt.rs` | 2,066 | `CollectionManager`: background tasks for introspection collections |
| `src/instance.rs` | 963 | `Instance`/`Replica`: storage cluster management + command replay |
| `src/persist_handles.rs` | 560 | `PersistTableWriteWorker`: serialized table writes via txn-wal |
| `src/history.rs` | 547 | `CommandHistory`: reducible command log for replica rehydration |
| `src/statistics.rs` | 261 | Background scrapers → introspection collections |
| `src/rtr.rs` | 201 | Real-time recency timestamp resolution |
| `src/persist_handles/read_only_table_worker.rs` | 292 | Read-only mode table-write fallback |

## Package identity

Crate name: `mz-storage-controller`. Key deps: `mz-storage-client`,
`mz-storage-types`, `mz-persist-client`, `mz-txn-wal`, `mz-service`,
`mz-cluster-client`.

## Purpose

Single-process implementation of the storage controller. Manages:
- Collection lifecycle (create/alter/drop for sources, sinks, tables).
- Storage cluster (`Instance`) and `Replica` connection management with
  `CommandHistory`-based rehydration.
- Introspection collection maintenance via `CollectionManager` (append-only
  and differential modes, read-only-aware).
- Table writes serialized through `PersistTableWriteWorker` → `TxnsHandle`.
- Statistics scraping and wallclock-lag tracking.
- Real-time recency (`rtr.rs`): frontier polling against the upstream source.

## Key interfaces (exported)

- `Controller::new(…, storage_collections)` — constructor; takes `StorageCollections`
  as an injected dependency.
- `impl StorageController for Controller` — the full trait surface defined in
  `mz-storage-client::controller`.

## Architecture notes

- **`lib.rs` is the monolith**: 4,070 LOC in a single file; `TODO(aljoscha):
  It would be swell if we could refactor this Leviathan` appears twice. It is
  the primary candidate for decomposition.
- **Injected `StorageCollections`**: `Controller` holds
  `Arc<dyn StorageCollections + Send + Sync>`, constructed externally by
  `mz-controller`. This seam enables the combined controller to share the
  `StorageCollectionsImpl` between storage and compute paths.
- **Dual collection stores**: `Controller.collections: BTreeMap<GlobalId,
  CollectionState>` tracks ingestion/export/wallclock-lag state; the injected
  `StorageCollections` tracks since/upper/ReadHold state. Keeping them in sync
  requires explicit cross-notifications on create/drop.
- **Storage replication gap**: `Instance` comment (line 49) notes that storage
  objects don't support replication (database-issues#5051). Multi-replica
  instances can only exist with zero storage objects installed.
- **`CollectionManager` read-only mode**: differential collections maintain
  `desired` state in memory during read-only mode and flush on promotion;
  append-only writes error out. This is the 0dt-upgrade path.
- **`PersistTableWriteWorker` and read-only mode**: falls back to
  `read_only_table_worker.rs` which accepts writes as batches but defers appends.
- **Open tech debt markers** (selected):
  - `TODO(alter_table)`: schema evolution for sources not yet supported.
  - `TODO(sinks)`: sink hydration tracking absent.
  - `TODO(cf2)`: oneshot ingestion error messages are stringly typed.
  - `TODO(guswynn): cluster-unification`: storage and compute cluster
    management are parallel; merge is tracked but not started.

## Downstream consumers

`mz-controller` (wraps both storage and compute controllers), `mz-environmentd`.

## Bubbled findings for src/CONTEXT.md

- `mz-storage-controller` contains the only concrete `StorageController` impl;
  its `lib.rs` (4,070 LOC) is the largest single file in this bundle and a
  recognized refactor target.
- The dual-`CollectionState` pattern (controller + `StorageCollections`) is a
  cross-crate coupling hotspot; changes to collection lifecycle require careful
  synchronization across both stores.
- Storage replication is explicitly unsupported and tracked in
  database-issues#5051 — callers must not install storage objects on
  multi-replica instances.
- `CollectionManager`'s read-only/0dt-upgrade path is the cleanest modular
  design in the crate; append-only vs differential split is well-documented.
