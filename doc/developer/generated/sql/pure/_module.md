---
source: src/sql/src/pure.rs
revision: b72bd8ad32
---

# mz-sql::pure

Implements SQL purification — the async pre-planning pass that inlines external state into SQL ASTs.
The root file (`pure.rs`) is the main entry point (`purify_statement`), handling Kafka, Postgres, MySQL, SQL Server, load generator, Avro/Protobuf schema registry, and Iceberg sink purification; it dispatches source-specific logic to `mysql`, `postgres`, and `sql_server` submodules.
The `references` submodule abstracts upstream reference retrieval across all source types, and `error` defines per-source purification error types.
For Postgres sources, purification retrieves the timeline ID via `get_timeline_id` and records whether the upstream server is a physical replica via `get_is_in_recovery`; both values are stored in `PostgresSourcePublicationDetails` so that the replication layer can use the appropriate LSN-loading method depending on whether the connection is to a primary or a standby.
For Iceberg sinks, `purify_create_sink` validates the catalog connection by calling `connect`, and validates the optional AWS storage connection (via `USING AWS CONNECTION`) if present by loading its SDK config; the AWS validation is skipped when no storage connection is specified.
`AvroSchema::Glue { .. }` in a source format raises a "not yet implemented" error at purification time.
