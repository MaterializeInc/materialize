---
source: src/storage/src/source/mysql/statistics.rs
revision: e79a6d96d9
---

# mz-storage::source::mysql::statistics

Renders the statistics operator for the MySQL source, which periodically queries `@@gtid_executed` to compute `offset_known` and `offset_committed` progress statistics, and emits `Probe<GtidPartition>` events to drive reclocking.
