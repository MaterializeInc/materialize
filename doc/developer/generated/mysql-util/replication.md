---
source: src/mysql-util/src/replication.rs
revision: 12fbe31d24
---

# mysql-util::replication

Provides helper functions that verify MySQL system variables required for CDC replication: `ensure_full_row_binlog_format` (checks `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`), `ensure_gtid_consistency` (GTID mode and enforcement), and `ensure_replication_commit_order` (guards against multi-threaded replica reordering).
Also exposes `query_sys_var` for safely reading individual system variables by name. Because system variable names must be interpolated unparameterized, `query_sys_var` uses `query_first` directly (rather than the prepared `exec_*` family) after validating the name through the `is_safe_sys_var_name` allowlist.
