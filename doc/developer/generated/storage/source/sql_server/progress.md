---
source: src/storage/src/source/sql_server/progress.rs
revision: af5f8f85e1
---

# mz-storage::source::sql_server::progress

Implements the progress-tracking operator for SQL Server CDC sources.
On startup, validates the upstream restore history ID against the value stored in `SqlServerSourceExtras`; if a restore is detected and `SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY` is enabled, the operator exits early so the replication operator's definite error can propagate downstream.
Periodically probes the upstream server for its current maximum LSN to compute `offset_known`, listens to the resume-upper stream to update `offset_committed`, and optionally cleans up already-ingested rows from the upstream change tables when `CDC_CLEANUP_CHANGE_TABLE` is enabled.
