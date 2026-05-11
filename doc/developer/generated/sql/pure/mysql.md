---
source: src/sql/src/pure/mysql.rs
revision: 16d611fb45
---

# mz-sql::pure::mysql

MySQL-specific purification helpers: converts `ExternalReferences` and `RetrievedSourceReferences` into `CreateSubsourceStatement` ASTs, validates table references (two-part names only), and maps MySQL column types to Materialize types.
Defines `MYSQL_DATABASE_FAKE_NAME` (the synthetic database name used to fit MySQL's two-layer hierarchy into the three-layer catalog model).
Exports two async helpers for verifying the upstream MySQL `binlog_row_metadata` setting: `ensure_binlog_full_metadata` (returns an error if the setting is not `FULL` or the server version is below 8.0.1) and `is_binlog_full_metadata` (returns a `bool`, treating non-fatal errors as `false` and propagating connection errors).
