---
source: src/testdrive/src/action/duckdb/query.rs
revision: fa066df3e5
---

# testdrive::action::duckdb::query

Implements the `duckdb-query` builtin command, which executes the first line of the input as a SQL query against a named DuckDB connection and compares the result rows against the remaining input lines.
Supports optional row sorting via the `sort-rows` argument; row values are stringified from `duckdb::types::ValueRef`.
