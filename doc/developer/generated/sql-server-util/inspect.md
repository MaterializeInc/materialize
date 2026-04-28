---
source: src/sql-server-util/src/inspect.rs
revision: 12e16abba3
---

# mz-sql-server-util::inspect

Contains queries that interrogate SQL Server system tables and CDC functions to support source ingestion.
Key functions include `get_min_lsn` / `get_min_lsns` / `get_max_lsn` (with retry variants), `increment_lsn`, `cleanup_change_table`, `get_changes_asc` (streaming CDC changes for a capture instance between two LSNs), `snapshot` (streaming full-table snapshot), `get_ddl_history` (DDL events for a capture instance), `get_schemas` / `get_table_descriptions` for schema discovery, and `get_constraints_for_tables` (retrieves PRIMARY KEY and UNIQUE constraints using `KEY_COLUMN_USAGE` joined to `TABLE_CONSTRAINTS`, ordered by `ORDINAL_POSITION` to preserve composite-key column order).
`get_min_lsns` reads `start_lsn` directly from `cdc.change_tables` for a batch of capture instances, bypassing the `sys.fn_cdc_get_min_lsn` NULL-return logic.
`parse_lsn` and `parse_numeric_lsn` handle the two LSN wire formats (binary bytes and `NUMERIC(25,0)`) returned by different SQL Server APIs.
`DDLEvent` represents a schema-change record retrieved from the CDC DDL history table.
