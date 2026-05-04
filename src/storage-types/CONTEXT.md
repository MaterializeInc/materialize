# mz-storage-types

Shared type definitions for all `mz-storage*` crates. Defines the data model
for sources, sinks, connections, and the storage controller protocol — the
interface layer between adapter, controller, and storage workers.

## Top-level files (LOC ≈ 9,210 in `src/*.rs`)

| File | What it owns |
|---|---|
| `connections.rs` | `Connection` enum; `ConnectionContext`; all connection `connect()` impls |
| `sources.rs` | `IngestionDescription`, `SourceData`, `MzOffset`, source traits |
| `errors.rs` | `DataflowError` taxonomy (decode, eval, source errors); proto encoding |
| `sinks.rs` | `StorageSinkDesc`, `StorageSinkConnection`, sink envelopes |
| `controller.rs` | `CollectionMetadata`, `StorageResponse`, `AlterError`; controller protocol types |
| `stats.rs` | `RelationPartStats` — bridges `mz-repr` types to persist `PartStats` for filter pushdown |
| `dyncfgs.rs` | Dynamic config constants for storage (Kafka reconnect backoff, address enforcement, etc.) |
| `parameters.rs` | `StorageParameters` — immutable config snapshot sent to storage workers at startup |
| `time_dependence.rs` | `TimeDependence` — model for wall-clock dependency of a collection's `since` frontier |
| `read_holds.rs` | `ReadHold` — RAII guard holding a collection's `since` frontier open |
| `read_policy.rs` | `ReadPolicy` — compaction policy (lag-based, step-back, etc.) |
| `instances.rs` | `StorageInstanceId` — typed ID for storage cluster instances |
| `oneshot_sources.rs` | Copy-from types for one-shot ingestion |
| `configuration.rs` | `StorageConfiguration` — combines parameters + connection context |
| `lib.rs` | `AlterCompatible` trait; `StorageDiff` type alias (`i64`) |

## Subdirectories

| Path | LOC | What it owns |
|---|---|---|
| `src/connections/` | 2,945 | Connection types and runtime context — see [`src/connections/CONTEXT.md`](src/connections/CONTEXT.md) |
| `src/sources/` | 3,562 | Source ingestion descriptors — see [`src/sources/CONTEXT.md`](src/sources/CONTEXT.md) |
| `src/sinks/` | 909 | Sink descriptor types — see [`src/sinks/CONTEXT.md`](src/sinks/CONTEXT.md) |

## Key concepts

- **`AlterCompatible` trait** — the central correctness contract: all connection/source/sink types implement it to declare which fields survive `ALTER`. Default impl is strict equality; override for fields that may change.
- **`StorageDiff`** — `i64` type alias; the diff type for all storage differential dataflow collections.
- **`RelationPartStats`** — the seam between `mz-repr`'s `RelationDesc` and persist's `PartStats`; enables `MFP` filter pushdown into S3 reads.
- **`ReadHold` / `ReadPolicy`** — the since-management primitives; `ReadHold` is RAII, `ReadPolicy` drives compaction scheduling.

## Key interfaces (exported)

- **`AlterCompatible`** — trait governing which fields may change across `ALTER`; the contract between adapter and storage.
- **`Connection`** — enum over all external-system connection types (Kafka, Postgres, MySQL, SQL Server, SSH, AWS, IcebergCatalog, …).
- **`IngestionDescription`** — full source plan passed from adapter through controller to rendering workers.
- **`SourceData`** — `Result<Row, DataflowError>` with Arrow columnar `Codec` impl; the unit persisted in source shards.
- **`StorageSinkDesc`** — full sink dataflow descriptor, generic over metadata and timestamp.
- **`ReadHold` / `ReadPolicy`** — RAII since-frontier hold and compaction policy.
- **`RelationPartStats`** — bridges `RelationDesc` + `PartStats` for persist filter pushdown.
- **`StorageDiff`** — `i64` type alias; all storage differential collections use this diff type.

## What to bubble up to src/CONTEXT.md

- `mz-storage-types` is the interface seam between the adapter/catalog layer and the storage execution layer; no storage crate implements logic that bypasses these types.
- `AlterCompatible` is the primary correctness invariant for online schema changes across the storage stack — every new connection/source/sink field must explicitly opt in or it defaults to immutable.

## Cross-references

- Generated developer docs: `doc/developer/generated/storage-types/`.
- `mz-repr`, `mz-persist-types`, `mz-expr` — foundational dependencies.
- Consumed by `mz-storage-client`, `mz-storage-controller`, `mz-adapter`, `mz-catalog`.
