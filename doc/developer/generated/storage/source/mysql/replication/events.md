---
source: src/storage/src/source/mysql/replication/events.rs
revision: b11c202bad
---

# mz-storage::source::mysql::replication::events

Implements binlog event handlers that decode `RowsEventData` (insert, update, delete) and `QueryEvent` (DDL) into `SourceMessage` records.
`DROP TABLE` handling uses `sqlparser` to parse the statement and extract all dropped table names (including multi-table `DROP TABLE t1, t2` forms); a helper `drop_table_identifiers` function performs this SQL parsing. Only source outputs whose `initial_gtid_set` is at or before the drop GTID are marked as errored via `DefiniteError::TableDropped`; outputs created after the drop GTID are unaffected. The `table_ident_from_object_name` helper converts a `sqlparser` `ObjectName` (which may carry `ObjectNamePart::Identifier` or `ObjectNamePart::Function` components) to a `MySqlTableName`, rejecting function-type name parts (a Snowflake-only concept) and names with other than one or two components.
Handles rewind logic to subtract snapshot data that overlaps with the replication stream.
