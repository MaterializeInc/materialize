---
source: src/sql-server-util/src/inspect.rs
revision: 77534a6502
---

# mz-sql-server-util::inspect

Contains queries that interrogate SQL Server system tables and CDC functions to support source ingestion.
Key functions include `get_min_lsn` / `get_max_lsn` (with retry variants), `get_changes_asc` (streaming CDC changes for a capture instance between two LSNs), `snapshot` (streaming full-table snapshot), `get_ddl_history` (DDL events for a capture instance), and `get_schemas` / `get_table_descriptions` for schema discovery.
`parse_lsn` and `parse_numeric_lsn` handle the two LSN wire formats (binary bytes and `NUMERIC(25,0)`) returned by different SQL Server APIs.
`DDLEvent` represents a schema-change record retrieved from the CDC DDL history table.
