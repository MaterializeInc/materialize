---
source: src/storage-types/src/sources/sql_server.rs
revision: f2656c001e
---

# storage-types::sources::sql_server

Defines `SqlServerSourceConnection`, `SqlServerSourceExportDetails`, and `SqlServerSourceExtras` for SQL Server CDC sources using LSN-based timestamps.
Declares several `mz_dyncfg::Config` constants controlling LSN wait timeout, snapshot progress reporting interval, and CDC change-table cleanup behaviour.
Implements `SourceConnection` and `SourceTimestamp` backed by `Lsn` from `mz_sql_server_util`.
