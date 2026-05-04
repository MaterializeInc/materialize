# storage-types::sources

Core source ingestion type definitions shared between adapter, storage controller,
and storage workers.

## Files (LOC ≈ 3,562 across this directory)

| File | What it owns |
|---|---|
| `sources.rs` (parent) | `IngestionDescription`, `SourceDesc`, `SourceExport`, `SourceData` (row-or-error, implements `mz_persist_types::Codec`), `MzOffset`; `SourceConnection` + `SourceTimestamp` traits |
| `casts.rs` | `StorageScalarExpr` — source-side cast expressions used in source export projections |
| `encoding.rs` | `SourceDataEncoding`, `DataEncoding`, per-format encoding descriptors (Avro, CSV, Protobuf, Regex, Bytes, Text) |
| `envelope.rs` | `SourceEnvelope`, `UnplannedSourceEnvelope`, `KeyEnvelope`; Debezium/Upsert/None variants |
| `kafka.rs` | `KafkaSourceConnection`, `KafkaSourceExportDetails`, `KafkaMetadataKind`, `KafkaTimestamp` |
| `load_generator.rs` | `LoadGeneratorSourceConnection`, `LoadGeneratorOutput`, `LoadGeneratorSourceExportDetails`; built-in generators (Auction, Counter, Marketing, TPCH, Clock) |
| `mysql.rs` | `MySqlSourceConnection`, `MySqlSourceExportDetails`, `GtidPartition` timestamp type |
| `postgres.rs` | `PostgresSourceConnection`, `PostgresSourceExportDetails`, `PostgresTimestamp` |
| `sql_server.rs` | `SqlServerSourceConnection`, `SqlServerSourceExportDetails`, `SqlServerTimestamp` |

## Key concepts

- **`SourceData`** — the fundamental persisted unit: `Result<Row, DataflowError>`. Uses a custom Arrow columnar codec with separate columns for data rows vs. error rows, enabling per-column filter pushdown into persist.
- **`IngestionDescription`** — the full source plan: connection config, export map, remap shard, source imports. Passed from adapter through storage controller to rendering workers.
- **`SourceConnection` / `SourceTimestamp` traits** — abstract over the five connector types; connector-specific structs implement both.
- **`AlterCompatible`** — all source connection types implement this; immutable fields (e.g. connector type, topic) return `AlterError` if changed across `ALTER SOURCE`.

## Cross-references

- `casts.rs` references `mz-expr::MirScalarExpr` — source side of the expression layer seam.
- `mz-persist-types::Codec` — implemented by `SourceData` here; consumed by `mz-persist-client`.
- Generated developer docs: `doc/developer/generated/storage-types/sources/`.
