---
source: src/testdrive/src/action/duckdb/query.rs
revision: 673fdb9d44
---

# testdrive::action::duckdb::query

Implements the `duckdb-query` builtin command, which executes the first line of the input as a SQL query against a named DuckDB connection and compares the result rows against the remaining input lines.
The query is retried with backoff until the result matches or the state timeout elapses, mirroring the behavior of testdrive's SQL queries; query-execution errors are retried as well.
Supports optional row sorting via the `sort-rows` argument; row values are stringified from `duckdb::types::ValueRef`.
