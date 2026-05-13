# storage-types::sinks

Sink descriptor types shared between adapter, storage controller, and storage workers.

## Files (LOC ≈ 909 across this directory)

| File | What it owns |
|---|---|
| `sinks.rs` (parent) | `StorageSinkDesc<S, T>` — full sink dataflow descriptor; `StorageSinkConnection` enum (Kafka, Iceberg); `SinkEnvelope` (Debezium, Upsert, Append); `KafkaSinkConnection`, `KafkaSinkFormat`, `KafkaSinkFormatType`, `KafkaSinkCompressionType`, `KafkaIdStyle`; `IcebergSinkConnection`; `ICEBERG_APPEND_DIFF_COLUMN` / `ICEBERG_APPEND_TIMESTAMP_COLUMN` constants; `S3UploadInfo`, `S3SinkFormat`, `MIN_S3_SINK_FILE_SIZE`, `MAX_S3_SINK_FILE_SIZE` |
| `s3_oneshot_sink.rs` | S3 preflight types used for copy-to-S3 sinks |

## Key concepts

- **`StorageSinkDesc<S, T>`** — generic over metadata type `S` (populated by storage controller with shard metadata) and timestamp `T`. Carries the full sink plan: source id, schema, connection, envelope, `as_of`, commit interval.
- **`StorageSinkConnection`** — two production backends: `Kafka` (streaming, Debezium/Upsert) and `Iceberg` (batch-append). The S3 path uses `s3_oneshot_sink` outside this enum.
- **`SinkEnvelope`** — `Debezium` (Kafka only), `Upsert`, `Append` (Iceberg only). Envelope is set at planning time and is immutable across `ALTER SINK`.
- **`AlterCompatible`** — `StorageSinkDesc` implements this; prevents changing connector type or incompatible envelope across alter.

## Cross-references

- `mz-pgcopy::CopyFormatParams` — used in `sinks.rs` for COPY TO format parameters.
- Generated developer docs: `doc/developer/generated/storage-types/sinks/`.
