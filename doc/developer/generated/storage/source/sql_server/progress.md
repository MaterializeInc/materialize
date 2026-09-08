---
source: src/storage/src/source/sql_server/progress.rs
revision: 946b68f676
---

# mz-storage::source::sql_server::progress

Implements the progress-tracking operator for SQL Server CDC sources.
On startup, validates the upstream restore history ID against the value stored in `SqlServerSourceExtras`; if a restore is detected and `SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY` is enabled, the operator exits early so the replication operator's definite error can propagate downstream.
On startup, fetches the upstream maximum LSN eagerly to seed both `offset_known` and `offset_committed` before entering the main loop: `offset_committed` is seeded from each output's resumption LSN (falling back to the current upstream max LSN when snapshotting), and `offset_known` is set to the current upstream max LSN; this prevents a large bogus ingestion-lag reading during the initial snapshot phase.
Periodically probes the upstream server for its current maximum LSN to update `offset_known`, listens to the resume-upper stream to update `offset_committed` (never regressing below the seeded value), and optionally cleans up already-ingested rows from the upstream change tables when `CDC_CLEANUP_CHANGE_TABLE` is enabled.
