---
source: src/mysql-util/src/replication.rs
revision: b3a16a93da
---

# mysql-util::replication

Provides helper functions that verify MySQL system variables required for CDC replication: `ensure_full_row_binlog_format` (checks `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`), `ensure_gtid_consistency` (GTID mode and enforcement), and `ensure_replication_commit_order` (guards against multi-threaded replica reordering).
Also exposes `query_sys_var` for safely reading individual system variables by name.
