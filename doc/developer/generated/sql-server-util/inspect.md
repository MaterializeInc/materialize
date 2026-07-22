---
source: src/sql-server-util/src/inspect.rs
revision: ddb8fab070
---

# mz-sql-server-util::inspect

Contains queries that interrogate SQL Server system tables and CDC functions to support source ingestion.
Key functions include `get_min_lsn` / `get_min_lsns` / `get_max_lsn` (with retry variants), `increment_lsn`, `cleanup_change_table`, `get_changes_asc` (streaming CDC changes for a capture instance between two LSNs), `snapshot` (streaming full-table snapshot), `get_ddl_history` (DDL events for a capture instance), `get_schemas` / `get_table_descriptions` for schema discovery, `get_constraints_for_tables` (retrieves PRIMARY KEY and UNIQUE constraints using `KEY_COLUMN_USAGE` joined to `TABLE_CONSTRAINTS`, ordered by `ORDINAL_POSITION` to preserve composite-key column order), and `get_engine_edition` (queries `SERVERPROPERTY('EngineEdition')` and returns an `EngineEdition` value).
`EngineEdition` enumerates the SQL Server engine editions (`PersonalOrDesktop`, `Standard`, `Enterprise`, `Express`, `AzureSqlDatabase`, `AzureSynapseAnalytics`, `AzureSqlManagedInstance`, `AzureSqlEdge`, `AzureSynapseServerlessSqlPool`, and `Other(i32)` for unrecognized values). It exposes `has_sql_server_agent()` and `has_restore_history()` to let callers skip checks that do not apply on Azure SQL Database, which has no SQL Server Agent and no accessible `msdb`. `get_latest_restore_history_id` and `ensure_sql_server_agent_running` both consult `engine_edition` and skip their queries when the edition does not support them.
`get_min_lsns` reads `start_lsn` directly from `cdc.change_tables` for a batch of capture instances, bypassing the `sys.fn_cdc_get_min_lsn` NULL-return logic.
`parse_lsn` and `parse_numeric_lsn` handle the two LSN wire formats (binary bytes and `NUMERIC(25,0)`) returned by different SQL Server APIs.
`DDLEvent` represents a schema-change record retrieved from the CDC DDL history table.
