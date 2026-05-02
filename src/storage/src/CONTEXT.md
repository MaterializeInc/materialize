# storage::src

The storage cluster crate: ingests external data into persist shards and exports
persist shards to external sinks, running as a timely dataflow cluster inside
`clusterd`.

## Files (LOC ≈ 36,651 total for this dir tree)

| File / Dir | LOC | What it owns |
|---|---|---|
| `storage_state.rs` + `storage_state/` | 1,447 + 542 | `Worker` + `StorageState` main loop; external→internal command bridge; `AsyncStorageWorker` companion |
| `upsert_continual_feedback_v2.rs` | 1,003 | Upsert-v2: differential-collection key state via persist feedback, selected by `ENABLE_UPSERT_V2` dyncfg |
| `upsert_continual_feedback.rs` | 1,311 | Upsert-v1b: persist feedback loop for rehydration; RocksDB/memory backends |
| `upsert.rs` + `upsert/` | 1,032 + 1,738 | Public entry points `upsert` / `upsert_v2`, dispatch shim; `UpsertKey`; `types`, `memory`, `rocksdb` backends |
| `statistics.rs` | 1,167 | `SourceStatistics` / `SinkStatistics` aggregators; prometheus mirroring |
| `healthcheck.rs` | 1,202 | `health_operator`; `StatusNamespace`; aggregates worker status → controller |
| `sink/` | 4,047 | `SinkRender` impls: `kafka.rs` (1,656), `iceberg.rs` (2,391); `sink.rs` 13-line dispatch |
| `render/` | 2,165 | Dataflow assembly: `sources.rs` (749), `sinks.rs` (215), `persist_sink.rs` (1,401); `render.rs` dispatch |
| `source/` | 16,866 | Raw-source framework + connectors (see `source/CONTEXT.md`) |
| `internal_control.rs` | 330 | `InternalStorageCommand` bus; `setup_command_sequencer`; `DataflowParameters` |
| `decode.rs` + `decode/` | 592 + 283 | `render_decode_delimited` / `render_decode_cdcv2`; avro, csv, protobuf sub-decoders |
| `server.rs` | 120 | `serve`: initialises timely cluster, calls `Worker::run` |
| `metrics/` | ~1,100 | Per-subsystem metric definitions (source, sink, upsert, decode) |

## Key concepts

- **External → Internal command split** — `StorageCommand` from the controller reaches `Worker::handle_storage_command` but never directly renders dataflows. It broadcasts an `InternalStorageCommand` through a timely sequencer dataflow so all workers see commands in the same deterministic order before dataflows are rendered.
- **`AsyncStorageWorker`** — companion thread for async operations (frontier lookups, catalog reads) that cannot block the synchronous timely main loop. Responses re-enter as `AsyncStorageWorkerResponse` and trigger internal command broadcast.
- **Two-scope ingestion pipeline** — source data lives in a `SourceTimeDomain` scope (`FromTime`); reclocking promotes it to `mz_repr::Timestamp`. Scope crossing is handled by `PusherCapture` (tokio channel). See `render.rs` module doc for the ASCII diagram.
- **Upsert triplication** — three co-existing implementations gated by `ENABLE_UPSERT_V2`:
  - `upsert` (classic): RocksDB/memory-backed state; rehydrates from persist feedback; older path.
  - `upsert_continual_feedback` (v1b): feedback-loop architecture, same backend as classic.
  - `upsert_continual_feedback_v2` (v2): differential-collection key state; no RocksDB dependency for key lookup; newer path.
  - Both `upsert` and `upsert_v2` call through `upsert.rs` shims that dispatch to v1b or v2 respectively.
- **`SinkRender` trait** *(in `mz_storage_operators`)* — symmetric to `SourceRender`; Kafka and Iceberg each implement it. Iceberg uses a 3-operator pipeline: `mint_batch_descriptions` → `write_data_files` → `commit_to_iceberg`.
- **`health_operator`** — aggregates `HealthStatusMessage` streams from all source/sink subgraphs into per-source `StatusUpdate` records; delays transient failures before triggering `SuspendAndRestart`.

## Bubbled-up context from subdirs

- `source/CONTEXT.md` — `SourceRender` trait; two-scope reclocking; `create_raw_source` as the generic assembler; seven load-generator impls.
- `source/generator/CONTEXT.md` — generator impls are pure `Iterator`s; `GeneratorKind` dispatch; `auction.rs` is bulk static data (not an architecture concern).

## Cross-references

- `mz_storage_client::client::{StorageCommand, StorageResponse}` — controller protocol.
- `mz_persist_client` — remap shards and persist sinks.
- `mz_storage_operators` — `persist_source`, `SinkRender`, `BackpressureMetrics` (separate crate).
- `clusterd` — crate entry point that calls `mz_storage::server::serve`.
- Generated developer docs: `doc/developer/generated/storage/`.
