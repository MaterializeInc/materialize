---
source: src/fivetran-destination/src/lib.rs
revision: 849327076c
---

# mz-fivetran-destination

Fivetran destination connector for Materialize. Implements the Fivetran Destination gRPC API, allowing Fivetran to replicate data into Materialize tables.

Public API: `MaterializeDestination` (the gRPC service implementation) and `DestinationConnectorServer` (the tonic server wrapper from `fivetran_sdk`).

## Module structure

* `destination` — `MaterializeDestination`: implements the `DestinationConnector` tonic trait. Handles `configuration_form`, `test`, `describe_table`, `create_table`, `alter_table`, `truncate_table`, `write_batch`, and `write_history_batch` RPCs.
  * `config` — connection configuration form definition and connection setup (`connect` returns a `tokio_postgres::Client`). Configuration fields: `host`, `user`, `password`, `dbname`, `schema`, `port`.
  * `ddl` — `describe_table`, `create_table`, `alter_table` implementations.
  * `dml` — `truncate_table` and `write_batch` implementations: reads batch files (CSV, with gzip/zstd decompression and AES-256-CBC decryption), maps Fivetran types to Materialize types, and applies upserts/deletes.
* `error` — `OpError` (wraps `OpErrorKind` plus a context stack) and `OpErrorKind` (enum covering field errors, postgres errors, filesystem errors, CSV errors, etc.). `OpErrorKind::can_retry()` identifies retryable failures (connection errors, duplicate-object on temporary resources, IO interrupts).
* `crypto` — `AsyncAesDecrypter<R>`: an `AsyncRead` adapter that AES-256-CBC-decrypts a stream on the fly using `openssl`.
* `fivetran_sdk` — generated tonic/prost bindings for the Fivetran SDK proto.
* `logging` — `FivetranLoggingFormat`: a `tracing_subscriber` `FormatEvent` that formats log events according to the Fivetran SDK logging format.
* `utils` — `is_system_column` (checks `_fivetran_` prefix), `to_materialize_type` (Fivetran `DataType` → Materialize SQL type name), `AsyncCsvReaderTableAdapter` (streams CSV bytes as `ByteRecord`s with column header mapping).

## Key dependencies

`tokio-postgres`, `postgres-openssl`, `tonic`, `prost`, `csv-async`, `openssl`, `mz-pgrepr`, `mz-sql-parser`.
