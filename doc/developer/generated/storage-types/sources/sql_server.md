---
source: src/storage-types/src/sources/sql_server.rs
revision: 1704d6481e
---

# storage-types::sources::sql_server

Defines `SqlServerSourceConnection`, `SqlServerSourceExportDetails`, and `SqlServerSourceExtras` for SQL Server CDC sources using LSN-based timestamps.
Declares several `mz_dyncfg::Config` constants controlling LSN wait timeout, snapshot progress reporting interval, and CDC change-table cleanup behaviour.
Implements `SourceConnection` and `SourceTimestamp` backed by `Lsn` from `mz_sql_server_util`.
`SqlServerSourceExportDetails` implements `AlterCompatible`, deferring field-level compatibility checks to the source render operators rather than performing them in the type system.
