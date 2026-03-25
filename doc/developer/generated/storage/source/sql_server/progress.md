---
source: src/storage/src/source/sql_server/progress.rs
revision: e79a6d96d9
---

# mz-storage::source::sql_server::progress

Implements the progress-tracking operator for SQL Server CDC sources.
Periodically probes the upstream server for its current maximum LSN to compute `offset_known`, listens to the resume-upper stream to update `offset_committed`, and optionally cleans up already-ingested rows from the upstream change tables when `CDC_CLEANUP_CHANGE_TABLE` is enabled.
