# mz-storage-types

Shared type definitions for all `mz-storage*` crates. Defines the data model
for sources, sinks, connections, and the storage controller protocol — the
interface layer between adapter, controller, and storage workers.

## Structure

| Path | LOC | What it owns |
|---|---|---|
| `src/connections/` | 2,945 | Connection types and runtime context — see [`src/connections/CONTEXT.md`](src/connections/CONTEXT.md) |
| `src/sources/` | 3,562 | Source ingestion descriptors — see [`src/sources/CONTEXT.md`](src/sources/CONTEXT.md) |
| `src/sinks/` | 909 | Sink descriptor types — see [`src/sinks/CONTEXT.md`](src/sinks/CONTEXT.md) |
| `src/` | 14,401 | All modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

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
