# storage::source

Raw-source ingestion framework: the generic pipeline from external data to
reclocked timely streams, plus all connector implementations.

## Files (LOC ≈ 16,866 total for this dir + generator subdir)

| File | What it owns |
|---|---|
| `source.rs` (parent, 44 LOC) | Module entry: re-exports `RawSourceCreationConfig`, `SourceExportCreationConfig`, `create_raw_source`, `KafkaSourceReader` |
| `source_reader_pipeline.rs` (675 LOC) | `create_raw_source` — the generic ingestion assembler: renders source in `SourceTimeDomain` scope, runs `remap_operator`, crosses scope boundary via `PusherCapture`/`reclock`, returns `IntoTime`-stamped collections |
| `types.rs` (244 LOC) | Core traits and types: `SourceRender`, `SourceMessage`, `SourceOutput`, `DecodeResult`, `Probe`, `StackedCollection`, `SignaledFuture` |
| `reclock.rs` (629 LOC) | `ReclockOperator` — mints `(FromTime, IntoTime, Diff)` bindings into remap shard; `compat::PersistHandle` adapter |
| `kafka.rs` (1,776 LOC) | `KafkaSourceReader` / `SourceRender` impl for Kafka; partition assignment, offset resumption, consumer group management |
| `postgres.rs` (522 LOC) | `SourceRender` for `PostgresSourceConnection`; replication slot management and WAL decoding |
| `generator.rs` (530 LOC) | `SourceRender` for `LoadGeneratorSourceConnection`; dispatches to `GeneratorKind` variants; `synthesize_probes` |
| `mysql.rs` (425 LOC) | `SourceRender` for `MySqlSourceConnection`; binlog-based replication |
| `sql_server.rs` (231 LOC) | `SourceRender` for `SqlServerSourceConnection`; CDC via SQL Server change tables |
| `probe.rs` (112 LOC) | `Ticker` — interval-based probe timer, rounds timestamps to interval boundary to reduce downstream churn |
| `generator/` | Seven load-generator dataset impls (see `generator/CONTEXT.md`) |

## Key concepts

- **`SourceRender` trait** — the single Interface every connector implements. Returns `(exports: BTreeMap<GlobalId, StackedCollection>, health: StreamVec, probes: StreamVec<Probe>, tokens: Vec<PressOnDropButton>)`. Timestamp type is an associated type `Self::Time: SourceTimestamp`.
- **Two-scope structure** — `create_raw_source` runs the connector in a `SourceTimeDomain` root scope (`FromTime`) and the reclocking/persistence in the parent scope (`mz_repr::Timestamp`). Scope crossing is done via `PusherCapture` (tokio unbounded channel) + `reclock` utility.
- **Remap shard / reclocking** — `remap_operator` writes `(FromTime → IntoTime)` bindings to a persist shard. A downstream `reclock` operator translates `FromTime`-stamped data to `IntoTime` using those bindings. `ReclockOperator` in `reclock.rs` does the minting; `compat::PersistHandle` is the Adapter to actual persist.
- **Probe stream** — connectors emit `Probe { probe_ts, upstream_frontier }` to report the upstream write frontier. The remap operator waits for new probes before minting bindings. Generators that have no upstream system use `synthesize_probes`.
- **`SignaledFuture`** — wraps async operator bodies with a `Semaphore`-based backpressure signal; upstream operators can slow down without blocking the timely thread.
- **`RawSourceCreationConfig`** — the fat config struct threaded through `create_raw_source` and `SourceRender::render`: source ID, exports, resume uppers, persist clients, statistics, remap metadata, busy signal.

## Cross-references

- `render/sources.rs` calls `source::create_raw_source` and then applies decode + upsert on top.
- `mz_storage_types::sources::{SourceConnection, SourceTimestamp, MzOffset}` — external protocol types.
- `mz_timely_util::reclock::reclock` — the `IntoTime` projection utility.
- Generated developer docs: `doc/developer/generated/storage/source/`.
