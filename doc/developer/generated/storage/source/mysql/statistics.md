---
source: src/storage/src/source/mysql/statistics.rs
revision: b382b15182
---

# mz-storage::source::mysql::statistics

Renders the statistics operator for the MySQL source, which periodically queries `@@gtid_executed` to compute `offset_known` and `offset_committed` progress statistics, and emits `Probe<GtidPartition>` events to drive reclocking.
The operator receives `ResumeUppers<GtidPartition>` values and reports each export's `offset_committed` individually from its per-export frontier.
