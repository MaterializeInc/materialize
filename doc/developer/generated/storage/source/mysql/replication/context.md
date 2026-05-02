---
source: src/storage/src/source/mysql/replication/context.rs
revision: 84b2975d9d
---

# mz-storage::source::mysql::replication::context

Defines `ReplContext`, a struct that bundles all mutable state needed while processing a MySQL binlog stream: the active `BinlogStream`, capability sets, pending rewinds, table name→output mappings, source metrics, and config.
Provides helper methods to drive stream consumption and capability management during replication event processing.
