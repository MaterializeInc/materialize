---
source: src/storage/src/source/sql_server/replication.rs
revision: e79a6d96d9
---

# mz-storage::source::sql_server::replication

Renders the replication operator for SQL Server CDC ingestion, polling change tables in LSN order and decoding rows into `SourceMessage` records.
Manages per-capture-instance LSN tracking, handles initial snapshot detection, and distributes output across timely workers by partition index.
