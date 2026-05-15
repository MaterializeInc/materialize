---
source: src/storage-types/src/sources.rs
revision: 16d611fb45
---

# storage-types::sources

Defines the core types for source ingestion: `IngestionDescription` (the full source plan including export map and metadata), `SourceDesc` (connection + encoding + envelope), `SourceExport`, `SourceData` (the row-or-error type persisted in source shards), and `MzOffset`.
The `SourceConnection` and `SourceTimestamp` traits abstract over the five concrete connector types (Kafka, Postgres, MySQL, SQL Server, load generator).
`SourceData` implements `mz_persist_types::Codec` using a custom columnar Arrow-based encoding that stores data rows and error rows in separate columns for efficient filter pushdown.
Submodules `casts`, `encoding`, `envelope`, `kafka`, `load_generator`, `mysql`, `postgres`, and `sql_server` each define the connection-specific structs for their respective source types; `casts` defines `StorageScalarExpr` for source cast expressions.
