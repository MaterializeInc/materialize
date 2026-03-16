---
source: src/storage-types/src/sources/mysql.rs
revision: 4267863081
---

# storage-types::sources::mysql

Defines `MySqlSourceConnection` and `MySqlSourceDetails` (table definitions for the snapshot), plus `MySqlSourceExportDetails` (per-table output column projection).
The timestamp type for MySQL sources is `Partitioned<GtidPartition, MzOffset>`, reflecting MySQL's GTID-based replication timeline.
Implements `SourceConnection` and `SourceTimestamp` for use in the generic ingestion pipeline.
