---
source: src/adapter-types/src/compaction.rs
revision: e757b4d11b
---

# mz-adapter-types::compaction

Defines `CompactionWindow`, an enum representing the logical compaction horizon for a collection: `Default` (1 s), `DisableCompaction` (no compaction), or `Duration(Timestamp)`.
Provides `lag_from` (compute the since frontier given a current timestamp) and `comparable_timestamp` (a `Timestamp` suitable for ordering windows).
Implements `From<CompactionWindow> for ReadPolicy<Timestamp>` so that compaction windows translate directly to storage read policies; all windows round since frontiers to the 1-second `SINCE_GRANULARITY`.
