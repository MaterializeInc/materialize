---
source: src/storage/src/source/mysql/statistics.rs
revision: 9e91428d8a
---

# mz-storage::source::mysql::statistics

Renders the statistics operator for the MySQL source, which periodically queries `@@gtid_executed` to compute `offset_known` and `offset_committed` progress statistics, and emits `Probe<GtidPartition>` events to drive reclocking.
