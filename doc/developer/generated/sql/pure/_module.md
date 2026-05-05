---
source: src/sql/src/pure.rs
revision: 16d611fb45
---

# mz-sql::pure

Implements SQL purification — the async pre-planning pass that inlines external state into SQL ASTs.
The root file (`pure.rs`) is the main entry point (`purify_statement`), handling Kafka, Postgres, MySQL, SQL Server, load generator, Avro/Protobuf schema registry, and Iceberg sink purification; it dispatches source-specific logic to `mysql`, `postgres`, and `sql_server` submodules.
The `references` submodule abstracts upstream reference retrieval across all source types, and `error` defines per-source purification error types.
