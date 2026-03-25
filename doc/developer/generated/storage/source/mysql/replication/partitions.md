---
source: src/storage/src/source/mysql/replication/partitions.rs
revision: 0ac4a9a3f1
---

# mz-storage::source::mysql::replication::partitions

Implements `GtidReplicationPartitions`, which maintains the complete frontier of `GtidPartition`s (covering all possible UUID ranges) as seen from the MySQL binlog stream.
As new GTIDs arrive, singleton partitions for known source-IDs are updated and missing UUID ranges are kept at `GtidState::Absent`, allowing the replication operator to downgrade its timely capabilities to a well-formed antichain.
