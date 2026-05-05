---
source: src/storage-types/src/sources/mysql.rs
revision: 16d611fb45
---

# storage-types::sources::mysql

Defines `MySqlSourceConnection` (holding a `CatalogItemId` connection reference and `MySqlSourceDetails`) and `MySqlSourceDetails` (an empty struct retained for conformity with other source types).
`MySqlSourceExportDetails` carries per-table export configuration: the `MySqlTableDesc`, the `initial_gtid_set` used as the effective snapshot point, `text_columns`, `exclude_columns`, and `binlog_full_metadata`.
The timestamp type for MySQL sources is `GtidPartition` (`Partitioned<Uuid, GtidState>`), reflecting MySQL's GTID-based replication timeline where each partition corresponds to a MySQL server UUID and the value is a `GtidState` (either `Absent` or `Active(NonZeroU64)`).
`gtid_set_frontier` parses a MySQL GTID set string into an `Antichain<GtidPartition>` representing the frontier of future transactions not yet seen.
Implements `SourceConnection` and `SourceTimestamp` for use in the generic ingestion pipeline.
