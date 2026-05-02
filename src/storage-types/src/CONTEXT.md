# storage-types::src (mz-storage-types)

Shared type definitions for all `mz-storage*` crates. The data model for
sources, sinks, connections, and the storage controller protocol.

## Subdirectories

| Path | What it owns |
|---|---|
| `connections/` | External-system connection types and runtime context — see [`connections/CONTEXT.md`](connections/CONTEXT.md) |
| `sources/` | Source ingestion descriptors and per-connector types — see [`sources/CONTEXT.md`](sources/CONTEXT.md) |
| `sinks/` | Sink descriptor types — see [`sinks/CONTEXT.md`](sinks/CONTEXT.md) |

## Top-level files (LOC ≈ 9,210 in src/*.rs)

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

## Key concepts

- **`AlterCompatible` trait** — the central correctness contract: all connection/source/sink types implement it to declare which fields survive `ALTER`. Default impl is strict equality; override for fields that may change.
- **`StorageDiff`** — `i64` type alias; the diff type for all storage differential dataflow collections.
- **`RelationPartStats`** — the seam between `mz-repr`'s `RelationDesc` and persist's `PartStats`; enables `MFP` filter pushdown into S3 reads.
- **`ReadHold` / `ReadPolicy`** — the since-management primitives; `ReadHold` is RAII, `ReadPolicy` drives compaction scheduling.

## Cross-references

- Consumed by `mz-storage-client`, `mz-storage-controller`, `mz-adapter`, `mz-catalog`, and rendering crates.
- `mz-repr`, `mz-persist-types`, `mz-expr` — core dependencies.
- Generated developer docs: `doc/developer/generated/storage-types/`.
