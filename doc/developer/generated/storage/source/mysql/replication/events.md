---
source: src/storage/src/source/mysql/replication/events.rs
revision: 5427dc5764
---

# mz-storage::source::mysql::replication::events

Implements binlog event handlers that decode `RowsEventData` (insert, update, delete) and `QueryEvent` (DDL) into `SourceMessage` records, applying schema verification on DDL events.
Handles rewind logic to subtract snapshot data that overlaps with the replication stream.
