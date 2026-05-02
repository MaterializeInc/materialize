# mz-storage

The `mz-storage` crate implements the storage layer of Materialize: it ingests
data from external systems into persist shards and exports persist shards to
external sinks, running as a timely dataflow cluster embedded in `clusterd`.

## Crate at a glance (LOC ≈ 38,273)

| Subtree | LOC |
|---|---|
| `src/source/` (connectors + raw-source framework) | ~16,866 |
| `src/` top-level files (upsert, render, storage_state, healthcheck, sink, …) | ~19,785 |

## What the crate owns

- **Cluster entry point** (`server.rs`): `serve` initialises the timely cluster and starts `Worker` loops.
- **Worker main loop** (`storage_state.rs`): `Worker` drives external `StorageCommand` processing, async frontier lookups (`AsyncStorageWorker`), and internal command sequencing. External commands never render dataflows directly — they broadcast `InternalStorageCommand` through a sequencer so all timely workers process them in deterministic order.
- **Ingestion dataflow assembly** (`render/sources.rs`): `render_source` wires raw source → decode → upsert/envelope → persist sink. Manages the two-scope scope structure (`FromTime` → `IntoTime` via reclocking).
- **Raw source framework** (`source/`): `SourceRender` trait + `create_raw_source` pipeline. Connectors: Kafka, Postgres, MySQL, SQL Server, load generators. Reclocking mints persist-backed `(FromTime → IntoTime)` bindings.
- **Upsert** (`upsert.rs`, `upsert_continual_feedback.rs`, `upsert_continual_feedback_v2.rs`): converts key-value update streams to differential collections; two implementations co-exist during migration (gated by `ENABLE_UPSERT_V2` dyncfg).
- **Sink implementations** (`sink/kafka.rs`, `sink/iceberg.rs`): `SinkRender` impls; Iceberg uses a 3-operator batch pipeline.
- **Health reporting** (`healthcheck.rs`): `health_operator` aggregates worker status updates and triggers `SuspendAndRestart` on failures.
- **Decode** (`decode.rs`, `decode/`): avro, csv, protobuf decoders applied after raw-source reclocking.
- **Statistics** (`statistics.rs`): source/sink progress metrics aggregated and mirrored to prometheus.

## Key architectural seams

- `SourceRender` — the Interface between the generic ingestion framework and per-connector implementations.
- `InternalStorageCommand` bus — the Seam that ensures timely-consistent ordering of dataflow-rendering commands across all workers.
- `ENABLE_UPSERT_V2` dyncfg — the Seam between the classic (RocksDB-backed) and v2 (differential) upsert implementations.

## Bubbled-up context from subdirs

- `src/CONTEXT.md` — full module map, upsert triplication, storage command architecture.
- `src/ARCH_REVIEW.md` — upsert triplication friction: three co-existing implementations; v1b deletion checklist (RocksDB, snapshot buffering, native merge operator must be verified before removal).
- `src/source/CONTEXT.md` — `SourceRender` trait, two-scope reclocking, `create_raw_source` assembler, probe/remap pipeline.
- `src/source/generator/CONTEXT.md` — load-generator impls; `auction.rs` is bulk static data.

## Cross-references

- `mz_storage_client` — controller ↔ worker protocol (`StorageCommand`, `StorageResponse`).
- `mz_storage_types` — DDL-level types (`IngestionDescription`, `StorageSinkDesc`, source/sink connection types).
- `mz_storage_operators` — shared dataflow operators (`persist_source`, `SinkRender`, backpressure metrics).
- `mz_persist_client` — remap shards and persist write/read.
- `clusterd` — embeds this crate and calls `serve`.
- Generated developer docs: `doc/developer/generated/storage/`.
