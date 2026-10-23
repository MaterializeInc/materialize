---
source: src/storage/src/source/sql_server/progress.rs
revision: 291766194b
---

# mz-storage::source::sql_server::progress

Implements the progress-tracking operator for SQL Server CDC sources.
On startup, validates the upstream restore history ID against the value stored in `SqlServerSourceExtras`; if a restore is detected and `SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY` is enabled, the operator exits early so the replication operator's definite error can propagate downstream.
On startup, fetches the upstream maximum LSN eagerly to seed both `offset_known` and `offset_committed` before entering the main loop: `offset_committed` is seeded from each output's resumption LSN (falling back to the current upstream max LSN when snapshotting), and `offset_known` is set to the current upstream max LSN; this prevents a large bogus ingestion-lag reading during the initial snapshot phase.
Periodically probes the upstream server for its current maximum LSN to update `offset_known`, and processes `ResumeUppers<Lsn>` to update each export's `offset_committed` individually (never regressing below the seeded value). When `CDC_CLEANUP_CHANGE_TABLE` is enabled, change-table cleanup is performed per capture instance: each instance's change table is cleaned up to the meet of the committed uppers of its exports only, computed by `committed_lsn`. A cleanup is skipped when the low-water mark has not advanced since the last successful cleanup, so a failed cleanup is retried once the low-water mark moves.
The `committed_lsn` helper computes the meet of the frontiers of a given set of export IDs within a `ResumeUppers::exports` map, returning `None` while any export has not yet committed beyond the as-of and once every export's upper is empty (when the source is dropped).
