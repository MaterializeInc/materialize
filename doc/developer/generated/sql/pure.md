---
source: src/sql/src/pure.rs
revision: fc8d9dc1e4
---

# mz-sql::pure

Main entry point for SQL purification: `purify_statement` is the async function that takes a raw `Statement<Raw>` and inlines all external state, returning a purified `Statement<Raw>` ready for planning.
Handles CREATE/ALTER SOURCE for Kafka (topic metadata, start-offset resolution, Avro/Protobuf schema registry), Postgres, MySQL, SQL Server, load generators, and Iceberg sinks.
Source-specific logic lives in the `mysql`, `postgres`, and `sql_server` submodules; `references` abstracts upstream catalog retrieval; `error` defines per-source error types.
