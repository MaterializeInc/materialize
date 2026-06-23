---
source: src/sql/src/pure.rs
revision: b72bd8ad32
---

# mz-sql::pure

Main entry point for SQL purification: `purify_statement` is the async function that takes a raw `Statement<Raw>` and inlines all external state, returning a purified `Statement<Raw>` ready for planning.
Handles CREATE/ALTER SOURCE for Kafka (topic metadata, start-offset resolution, Avro/Protobuf schema registry), Postgres, MySQL, SQL Server, load generators, and Iceberg sinks.
Source-specific logic lives in the `mysql`, `postgres`, and `sql_server` submodules; `references` abstracts upstream catalog retrieval; `error` defines per-source error types.
For Postgres sources, purification retrieves the timeline ID via `get_timeline_id` and records whether the upstream server is a physical replica via `get_is_in_recovery`; both values are stored in `PostgresSourcePublicationDetails` so that the replication layer can use the appropriate LSN-loading method depending on whether the connection is to a primary or a standby.
For Iceberg sinks, `purify_create_sink` validates the catalog connection by calling `connect`, and validates the optional AWS storage connection (via `USING AWS CONNECTION`) if present by loading its SDK config; the AWS validation is skipped when no storage connection is specified.
`AvroSchema::Glue { .. }` in a source format raises a "not yet implemented" error at purification time.
