---
source: src/storage-types/src/sources.rs
revision: bbe8b6977e
---

# storage-types::sources

Defines the core types for source ingestion: `IngestionDescription` (the full source plan including export map and metadata), `SourceDesc` (connection + encoding + envelope), `SourceExport`, `SourceData` (the row-or-error type persisted in source shards), and `MzOffset`.
The `SourceConnection` and `SourceTimestamp` traits abstract over the five concrete connector types (Kafka, Postgres, MySQL, SQL Server, load generator).
`SourceData` implements `mz_persist_types::Codec` using a custom columnar Arrow-based encoding that stores data rows and error rows in separate columns for efficient filter pushdown.
Submodules `casts`, `encoding`, `envelope`, `kafka`, `load_generator`, `mysql`, `postgres`, and `sql_server` each define the connection-specific structs for their respective source types; `casts` defines `StorageScalarExpr` for source cast expressions.
`SourceExportStatementDetails::Postgres` carries a `cast_oid_full_range: bool` field. Exports purified before the OID cast was widened decode this as `false` and use the legacy `i32`-range cast; newly purified exports set it to `true` and use the full `u32`-range cast.
For old-syntax sources, the `source_exports` field in `IngestionDescription` includes the primary source's ID in addition to explicit export IDs; callers that need only ingestion exports must filter it out. New-syntax sources (with source tables) list only their explicit exports in `source_exports`, so the primary collection ID is not present there.
