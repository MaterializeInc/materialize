---
source: src/sql/src/pure.rs
revision: 4c35690026
---

# mz-sql::pure

Main entry point for SQL purification: `purify_statement` is the async function that takes a raw `Statement<Raw>` and inlines all external state, returning a purified `Statement<Raw>` ready for planning.
Handles CREATE/ALTER SOURCE for Kafka (topic metadata, start-offset resolution, Avro/Protobuf schema registry), Postgres, MySQL, SQL Server, load generators, and Iceberg sinks.
Source-specific logic lives in the `mysql`, `postgres`, and `sql_server` submodules; `references` abstracts upstream catalog retrieval; `error` defines per-source error types.
For Iceberg sinks, `purify_create_sink` validates the catalog connection by calling `connect`, and validates the optional AWS storage connection (via `USING AWS CONNECTION`) if present by loading its SDK config; the AWS validation is skipped when no storage connection is specified.
